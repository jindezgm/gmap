/*
 * @Author: jinde.zgm
 * @Date: 2020-07-28 22:02:49
 * @Descripttion:
 */

package gmap

import (
	"fmt"
	"reflect"
	"sync"

	"go.etcd.io/etcd/pkg/pbutil"
)

// typedMap is map[string]interface{}, value type is one value type when creating gmap
type typedMap struct {
	name     string                     // Map name
	gmap     *gmap                      // gmap pointer
	vtype    *valueType                 // Value type
	mutex    sync.RWMutex               // Map data lock
	data     map[string]revisionedValue // Map data
	watchers sync.Map                   // Watchers
}

// typedMap impelement Map interfaces.
var _ Map = &typedMap{}

// newTypedMap create typedMap.
func newTypedMap(gm *gmap, name string, vtype *valueType) *typedMap {
	return &typedMap{
		gmap:  gm,
		name:  name,
		vtype: vtype,
		data:  make(map[string]revisionedValue),
	}
}

// Name implement Map.Name()
func (m *typedMap) Name() string { return m.name }

// Get implement Map.Get()
func (m *typedMap) Get(key string) (interface{}, bool) {
	m.mutex.RLock()
	value, exist := m.data[key]
	m.mutex.RUnlock()

	return value.Value, exist
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
func (m *typedMap) Range(begin, end string, f func(key string, value interface{}) bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// If the key range is not specified, iterate the map directly
	if begin == "" && end == "" {
		for k, v := range m.data {
			if !f(k, v.Value) {
				break
			}
		}
	} else {
		// Only output key-value which key in [begin, end)
		for k, v := range m.data {
			if k >= begin && k < end {
				if !f(k, v) {
					break
				}
			}
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
func (m *typedMap) GuaranteedUpdate(key string, ignoredNotFound bool, tryUpdate TryUpdateFunc) error {
	for {
		// Get current value.
		m.mutex.RLock()
		value, exist := m.data[key]
		m.mutex.RUnlock()
		// Check value exist.
		if !exist && !ignoredNotFound {
			return fmt.Errorf("key is not exist:%s", key)
		}
		// Try update.
		updated, err := tryUpdate(value.Value)
		if nil != err {
			return err
		}
		// Submit update proposal.
		id := m.gmap.idGen.Next()
		ar, err := m.gmap.propose(id, m.newUpdateProposal(key, updated, value.Revision, id))
		if nil != err { // Any error ?
			return err
		} else if ar.rev != 0 { // ar.rev is new revision for updating value, !=0 mean update succeeded.
			break
		}
	}

	return nil
}

// Watch impelent Map.Watch()
func (m *typedMap) Watch() Watcher {
	w := newWatcher(m)
	m.watchers.Store(w, nil)
	return w
}

// put apply put request.
func (m *typedMap) put(key string, value interface{}, revision uint64) {
	m.mutex.Lock()
	m.data[key] = revisionedValue{Value: value, Revision: revision}
	m.mutex.Unlock()
}

// delete apply delete request.
func (m *typedMap) delete(key string) revisionedValue {
	m.mutex.Lock()
	pre := m.data[key]
	delete(m.data, key)
	m.mutex.Unlock()

	return pre
}

// update apply update request.
func (m *typedMap) update(key string, value interface{}, oldrev, newrev uint64) (revisionedValue, bool) {
	m.mutex.Lock()
	pre := m.data[key]
	// Update only if same revision.
	if pre.Revision == oldrev {
		m.data[key] = revisionedValue{Value: value, Revision: newrev}
	}
	m.mutex.Unlock()

	return pre, pre.Revision == oldrev
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
	m.mutex.RLock()
	defer m.mutex.Unlock()

	switch {
	// Protobufable value object.
	case m.vtype.ptype != nil:
		// Convert map[string]revisionedValue to ProtoValueMap
		pvm := ProtoValueMap{Map: m.name, Data: make(map[string]RevisionedProtoValue, len(m.data))}
		for k, v := range m.data {
			pvm.Data[k] = RevisionedProtoValue{Revision: v.Revision, Value: v.Value.(ProtoValue)}
		}
		ms.Proto = append(ms.Proto, pvm)
	// String value.
	case m.vtype.vtype.Kind() == reflect.String:
		// Convert map[string]revisionedValue to StringValueMap
		svm := StringValueMap{Map: m.name, Data: make(map[string]RevisionedStringValue, len(m.data))}
		for k, v := range m.data {
			svm.Data[k] = RevisionedStringValue{Revision: v.Revision, Value: v.Value.(string)}
		}
		ms.String_ = append(ms.String_, svm)
	// []byte value.
	case m.vtype.vtype.Kind() == reflect.Slice && m.vtype.vtype.Elem().Kind() == reflect.Uint8:
		// Convert map[string]revisionedValue to BytesValueMap
		bvm := BytesValueMap{Map: m.name, Data: make(map[string]RevisionedBytesValue, len(m.data))}
		for k, v := range m.data {
			bvm.Data[k] = RevisionedBytesValue{Revision: v.Revision, Value: v.Value.([]byte)}
		}
		ms.Bytes = append(ms.Bytes, bvm)
	// User defined type
	default:
		// Convert map[string]revisionedValue to NormalValueMap
		nvm := NormalValueMap{Map: m.name, Data: mustMarshal(m.data)}
		ms.Normal = append(ms.Normal, nvm)
	}
}
