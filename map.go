/*
 * @Author: jinde.zgm
 * @Date: 2020-07-28 22:02:49
 * @Descripttion:
 */

package gmap

import (
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"go.etcd.io/etcd/pkg/pbutil"
)

// typedMap is map[string]interface{}, value type is one value type when creating gmap
type typedMap struct {
	x        sync.Map
	name     string     // Map name
	gmap     *gmap      // gmap pointer
	vtype    *valueType // Value type
	watchers sync.Map   // Watchers

	// The following code is copied from sync.Map
	mu sync.RWMutex
	// read contains the portion of the map's contents that are safe for
	// concurrent access (with or without mu held).
	//
	// The read field itself is always safe to load, but must only be stored with
	// mu held.
	//
	// Entries stored in read may be updated concurrently without mu, but updating
	// a previously-expunged entry requires that the entry be copied to the dirty
	// map and unexpunged with mu held.
	read atomic.Value // readOnly
	// dirty contains the portion of the map's contents that require mu to be
	// held. To ensure that the dirty map can be promoted to the read map quickly,
	// it also includes all of the non-expunged entries in the read map.
	//
	// Expunged entries are not stored in the dirty map. An expunged entry in the
	// clean map must be unexpunged and added to the dirty map before a new value
	// can be stored to it.
	//
	// If the dirty map is nil, the next write to the map will initialize it by
	// making a shallow copy of the clean map, omitting stale entries.
	dirty map[string]*entry
	// misses counts the number of loads since the read map was last updated that
	// needed to lock mu to determine whether the key was present.
	//
	// Once enough misses have occurred to cover the cost of copying the dirty
	// map, the dirty map will be promoted to the read map (in the unamended
	// state) and the next store to the map will make a new dirty copy.
	misses int
}

// readOnly is an immutable struct stored atomically in the Map.read field.
type readOnly struct {
	m       map[string]*entry
	amended bool // true if the dirty map contains some key not in m.
}

// expunged is an arbitrary pointer that marks entries which have been deleted
// from the dirty map.
var expunged = unsafe.Pointer(new(interface{}))

// An entry is a slot in the map corresponding to a particular key.
type entry struct {
	// p points to the interface{} value stored for the entry.
	//
	// If p == nil, the entry has been deleted and m.dirty == nil.
	//
	// If p == expunged, the entry has been deleted, m.dirty != nil, and the entry
	// is missing from m.dirty.
	//
	// Otherwise, the entry is valid and recorded in m.read.m[key] and, if m.dirty
	// != nil, in m.dirty[key].
	//
	// An entry can be deleted by atomic replacement with nil: when m.dirty is
	// next created, it will atomically replace nil with expunged and leave
	// m.dirty[key] unset.
	//
	// An entry's associated value can be updated by atomic replacement, provided
	// p != expunged. If p == expunged, an entry's associated value can be updated
	// only after first setting m.dirty[key] = e so that lookups using the dirty
	// map find the entry.
	p unsafe.Pointer // *interface{}
}

func newEntry(value *revisionedValue) *entry {
	return &entry{p: unsafe.Pointer(value)}
}

func (e *entry) load() (value *revisionedValue, ok bool) {
	p := atomic.LoadPointer(&e.p)
	if p == nil || p == expunged {
		return nil, false
	}
	return (*revisionedValue)(p), true
}

// tryStore stores a value if the entry has not been expunged.
//
// If the entry is expunged, tryStore returns false and leaves the entry
// unchanged.
func (e *entry) tryStore(value *revisionedValue) bool {
	for {
		p := atomic.LoadPointer(&e.p)
		if p == expunged {
			return false
		}
		if atomic.CompareAndSwapPointer(&e.p, p, unsafe.Pointer(value)) {
			return true
		}
	}
}

// unexpungeLocked ensures that the entry is not marked as expunged.
//
// If the entry was previously expunged, it must be added to the dirty map
// before m.mu is unlocked.
func (e *entry) unexpungeLocked() (wasExpunged bool) {
	return atomic.CompareAndSwapPointer(&e.p, expunged, nil)
}

// storeLocked unconditionally stores a value to the entry.
//
// The entry must be known not to be expunged.
func (e *entry) storeLocked(value *revisionedValue) {
	atomic.StorePointer(&e.p, unsafe.Pointer(value))
}

func (e *entry) delete() *revisionedValue {
	for {
		p := atomic.LoadPointer(&e.p)
		if p == nil || p == expunged {
			return nil
		}
		if atomic.CompareAndSwapPointer(&e.p, p, nil) {
			return (*revisionedValue)(p)
		}
	}
}

func (m *typedMap) missLocked() {
	m.misses++
	if m.misses < len(m.dirty) {
		return
	}
	m.read.Store(readOnly{m: m.dirty})
	m.dirty = nil
	m.misses = 0
}

func (m *typedMap) dirtyLocked() {
	if m.dirty != nil {
		return
	}

	read, _ := m.read.Load().(readOnly)
	m.dirty = make(map[string]*entry, len(read.m))
	for k, e := range read.m {
		if !e.tryExpungeLocked() {
			m.dirty[k] = e
		}
	}
}

func (e *entry) tryExpungeLocked() (isExpunged bool) {
	p := atomic.LoadPointer(&e.p)
	for p == nil {
		if atomic.CompareAndSwapPointer(&e.p, nil, expunged) {
			return true
		}
		p = atomic.LoadPointer(&e.p)
	}
	return p == expunged
}

// load copy the implementation of Load(), but return entry not value
func (m *typedMap) load(key string) (*entry, bool) {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]
	if !ok && read.amended {
		m.mu.Lock()
		// Avoid reporting a spurious miss if m.dirty got promoted while we were
		// blocked on m.mu. (If further loads of the same key will not miss, it's
		// not worth copying the dirty map for this key.)
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		if !ok && read.amended {
			e, ok = m.dirty[key]
			// Regardless of whether the entry was present, record a miss: this key
			// will take the slow path until the dirty map is promoted to the read
			// map.
			m.missLocked()
		}
		m.mu.Unlock()
	}

	return e, ok
}

func (m *typedMap) readOnly() readOnly {
	// We need to be able to iterate over all of the keys that were already
	// present at the start of the call to Range.
	// If read.amended is false, then read.m satisfies that property without
	// requiring us to hold m.mu for a long time.
	read, _ := m.read.Load().(readOnly)
	if read.amended {
		// m.dirty contains keys not in read.m. Fortunately, Range is already O(N)
		// (assuming the caller does not break out early), so a call to Range
		// amortizes an entire copy of the map: we can promote the dirty copy
		// immediately!
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		if read.amended {
			read = readOnly{m: m.dirty}
			m.read.Store(read)
			m.dirty = nil
			m.misses = 0
		}
		m.mu.Unlock()
	}

	return read
}

//-----------------------------------------------------------------------------------------------------
// typedMap implement Map interfaces.
var _ Map = &typedMap{}

// newTypedMap create typedMap.
func newTypedMap(gm *gmap, name string, vtype *valueType) *typedMap {
	return &typedMap{
		gmap:  gm,
		name:  name,
		vtype: vtype,
	}
}

// Name implement Map.Name()
func (m *typedMap) Name() string { return m.name }

// Get implement Map.Get()
func (m *typedMap) Get(key string) (interface{}, bool) {
	e, ok := m.load(key)
	if !ok {
		return nil, false
	}
	v, ok := e.load()
	if !ok {
		return nil, false
	}
	return v.Value, true
}

// Put implement Map.Put()
func (m *typedMap) Put(key string, value interface{}) error {
	// Create raft proposal ID and add into waiting list.
	id := m.gmap.idGen.Next()
	ar, err := m.gmap.propose(id, m.newPutProposal(key, value, id))
	if nil != err {
		return err
	}
	return ar.err
}

// Range implement Map.Range()
func (m *typedMap) Range(f func(key string, value interface{}) bool) {
	read := m.readOnly()
	for k, e := range read.m {
		v, ok := e.load()
		if !ok {
			continue
		}
		if !f(k, v.Value) {
			break
		}
	}
}

// Delete implement Map.Delete()
func (m *typedMap) Delete(key string) (interface{}, error) {
	// Create raft proposal ID and add into waiting list.
	id := m.gmap.idGen.Next()
	ar, err := m.gmap.propose(id, m.newDeleteProposal(key, id))
	if nil != err {
		return nil, err
	}
	return ar.pre.Value, ar.err
}

// GuaranteedUpdate implement Map.GuaranteedUpdate()
func (m *typedMap) GuaranteedUpdate(key string, tryUpdate TryUpdateFunc) error {
	for {
		var rv *revisionedValue

		// Get entry and check value exist or not.
		e, ok := m.load(key)
		if ok {
			rv, ok = e.load()
		}
		// If the specified key does not exist, equivalent to m.LoadOrStore.
		var value interface{}
		var revision uint64
		if ok {
			value, revision = rv.Value, rv.Revision
		}
		//
		value, err := tryUpdate(value)
		if nil != err {
			return err
		}
		//
		id := m.gmap.idGen.Next()
		_, err = m.gmap.propose(id, m.newUpdateProposal(key, value, revision, id))
		if nil != err {
			return err
		}
	}
}

// Watch impelent Map.Watch()
func (m *typedMap) Watch() Watcher {
	w := newWatcher(m)
	m.watchers.Store(w, nil)
	return w
}

func (m *typedMap) copy(data map[string]*entry) {
	m.mu.Lock()
	m.read.Store(readOnly{m: data})
	m.dirty, m.misses = nil, 0
	m.mu.Unlock()
}

// put apply put request.
func (m *typedMap) put(key string, value interface{}, revision uint64) {
	rv := &revisionedValue{Value: value, Revision: revision}
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok && e.tryStore(rv) {
		return
	}

	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		if e.unexpungeLocked() {
			// The entry was previously expunged, which implies that there is a
			// non-nil dirty map and this entry is not in it.
			m.dirty[key] = e
		}
		e.storeLocked(rv)
	} else if e, ok := m.dirty[key]; ok {
		e.storeLocked(rv)
	} else {
		if !read.amended {
			// We're adding the first new key to the dirty map.
			// Make sure it is allocated and mark the read-only map as incomplete.
			m.dirtyLocked()
			m.read.Store(readOnly{m: read.m, amended: true})
		}
		m.dirty[key] = newEntry(rv)
	}
	m.mu.Unlock()
}

// delete apply delete request.
func (m *typedMap) delete(key string) *revisionedValue {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]
	if !ok && read.amended {
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		if !ok && read.amended {
			delete(m.dirty, key)
		}
		m.mu.Unlock()
	}
	if ok {
		return e.delete()
	}
	return nil
}

// update apply update request.
func (m *typedMap) update(key string, value interface{}, oldrev, newrev uint64) (*revisionedValue, bool) {
	new := &revisionedValue{Value: value, Revision: newrev}
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		for {
			p := atomic.LoadPointer(&e.p)
			if p == expunged {
				break
			}
			old := (*revisionedValue)(p)
			if (nil == p && oldrev != 0) || old.Revision != oldrev {
				return nil, false
			}
			if atomic.CompareAndSwapPointer(&e.p, p, unsafe.Pointer(new)) {
				return old, true
			}
		}
	}

	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		if oldrev == 0 {
			if atomic.CompareAndSwapPointer(&e.p, expunged, unsafe.Pointer(new)) {
				// The entry was previously expunged, which implies that there is a
				// non-nil dirty map and this entry is not in it.
				m.dirty[key] = e
				return nil, true
			}
			return nil, false
		}
		return e.tryUpdate(new, oldrev)
	} else if e, ok := m.dirty[key]; ok {
		return e.tryUpdate(new, oldrev)
	} else if oldrev == 0 {
		if !read.amended {
			// We're adding the first new key to the dirty map.
			// Make sure it is allocated and mark the read-only map as incomplete.
			m.dirtyLocked()
			m.read.Store(readOnly{m: read.m, amended: true})
		}
		m.dirty[key] = newEntry(new)
	}
	m.mu.Unlock()

	return nil, false
}

func (e *entry) tryUpdate(new *revisionedValue, oldrev uint64) (*revisionedValue, bool) {
	p := atomic.LoadPointer(&e.p)
	old := (*revisionedValue)(p)
	if old.Revision == oldrev && atomic.CompareAndSwapPointer(&e.p, p, unsafe.Pointer(new)) {
		return old, false
	}
	return nil, false
}

// newPutProposal create raft put proposal.
func (m *typedMap) newPutProposal(key string, value interface{}, id uint64) []byte {
	request := InternalRaftRequest{
		ID: id,
		Put: &PutRequest{
			SetMapKey: SetMapKey{
				Set: m.vtype.vtype.String(),
				Map: m.name,
				Key: key,
			},
			Value: m.vtype.wrap(value),
		},
	}

	return pbutil.MustMarshal(&request)
}

// newDeleteProposal create raft delete proposal.
func (m *typedMap) newDeleteProposal(key string, id uint64) []byte {
	request := InternalRaftRequest{
		ID: id,
		Delete: &DeleteRequest{
			SetMapKey: SetMapKey{
				Set: m.vtype.vtype.String(),
				Map: m.name,
				Key: key,
			},
		},
	}

	return pbutil.MustMarshal(&request)
}

// newDeleteProposal create raft delete proposal.
func (m *typedMap) newUpdateProposal(key string, value interface{}, revision, id uint64) []byte {
	request := InternalRaftRequest{
		ID: id,
		Update: &UpdateRequest{
			SetMapKey: SetMapKey{
				Set: m.vtype.vtype.String(),
				Map: m.name,
				Key: key,
			},
			Revision: revision,
			Value:    m.vtype.wrap(value),
		},
	}

	return pbutil.MustMarshal(&request)
}

// addToMaps add typed map to maps.
func (m *typedMap) addToMaps(ms *Maps) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	switch {
	// Protobufable value object.
	case m.vtype.ptype != nil:
		// Convert map[string]revisionedValue to ProtoValueMap
		read := m.readOnly()
		pvm := ProtoValueMap{Map: m.name, Data: make(map[string]RevisionedProtoValue, len(read.m))}
		for k, e := range read.m {
			if v, ok := e.load(); ok {
				pvm.Data[k] = RevisionedProtoValue{Revision: v.Revision, Value: v.Value.(ProtoValue)}
			}
		}
		ms.Proto = append(ms.Proto, pvm)
	// String value.
	case m.vtype.vtype.Kind() == reflect.String:
		// Convert map[string]revisionedValue to StringValueMap
		read := m.readOnly()
		svm := StringValueMap{Map: m.name, Data: make(map[string]RevisionedStringValue, len(read.m))}
		for k, e := range read.m {
			if v, ok := e.load(); ok {
				svm.Data[k] = RevisionedStringValue{Revision: v.Revision, Value: v.Value.(string)}
			}
		}
		ms.String_ = append(ms.String_, svm)
	// []byte value.
	case m.vtype.vtype.Kind() == reflect.Slice && m.vtype.vtype.Elem().Kind() == reflect.Uint8:
		// Convert map[string]revisionedValue to BytesValueMap
		read := m.readOnly()
		bvm := BytesValueMap{Map: m.name, Data: make(map[string]RevisionedBytesValue, len(read.m))}
		for k, e := range read.m {
			if v, ok := e.load(); ok {
				bvm.Data[k] = RevisionedBytesValue{Revision: v.Revision, Value: v.Value.([]byte)}
			}
		}
		ms.Bytes = append(ms.Bytes, bvm)
	// User defined type
	default:
		// Convert map[string]revisionedValue to NormalValueMap
		read := m.readOnly()
		data := make(map[string]*revisionedValue, len(read.m))
		for k, e := range read.m {
			if v, ok := e.load(); ok {
				data[k] = v
			}
		}
		nvm := NormalValueMap{Map: m.name, Data: mustMarshal(data)}
		ms.Normal = append(ms.Normal, nvm)
	}
}
