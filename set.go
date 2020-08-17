/*
 * @Author: jinde.zgm
 * @Date: 2020-07-28 21:11:12
 * @Descripttion:
 */

package gmap

import (
	"reflect"
	"sync"
	"unsafe"
)

// set is a collection of maps of the same value type
type set struct {
	vtype *valueType // Value type
	gmap  *gmap      // map pointer
	maps  sync.Map   // All maps, key is map name.
}

// newSet create set by value type.
func newSet(typ reflect.Type, gm *gmap) *set {
	// Create set.
	s := &set{vtype: newValueType(typ), gmap: gm}
	// A set has only one value type, so insert valueTypes here.
	valueTypes.Store(typ.String(), s.vtype)

	return s
}

// get typed map by name.
func (s *set) get(name string) *typedMap {
	m, exist := s.maps.Load(name)
	if !exist {
		m, _ = s.maps.LoadOrStore(name, newTypedMap(s.gmap, name, s.vtype))
	}

	return m.(*typedMap)
}

// Sets is a collection of different value types, the key is reflect.TypeOf(value).String()
type sets map[string]*set

// save serialize sets into binary data.
func (ss *sets) save() ([]byte, error) {
	// Add all map into maps.
	var maps Maps
	for _, s := range *ss {
		s.maps.Range(func(key, value interface{}) bool {
			value.(*typedMap).addToMaps(&maps)
			return true
		})
	}
	// Marshal maps.
	return maps.Marshal()
}

// load deserialize set from data.
func (ss *sets) load(data []byte) error {
	// Unmarshal maps.
	var maps Maps
	if err := maps.Unmarshal(data); nil != err {
		return err
	}
	// Restore protobufable value map.
	for i := range maps.Proto {
		// Get set.
		set, exist := (*ss)[maps.Proto[i].Set]
		if !exist {
			logger.Panicf("set is not exist:%s", maps.Proto[i].Set)
		}
		// Convert ProtoValueMap to map[string]*entry
		data := make(map[string]*entry, len(maps.Proto[i].Data))
		for k, v := range maps.Proto[i].Data {
			data[k] = &entry{p: unsafe.Pointer(&revisionedValue{Revision: v.Revision, Value: v.Value})}
		}
		set.get(maps.Proto[i].Map).copy(data)
	}
	// Restore string value map.
	for i := range maps.String_ {
		// Get set.
		set, exist := (*ss)[maps.String_[i].Set]
		if !exist {
			logger.Panicf("set is not exist:%s", maps.String_[i].Set)
		}
		// Convert StringValueMap to map[string]*entry
		data := make(map[string]*entry, len(maps.String_[i].Data))
		for k, v := range maps.String_[i].Data {
			data[k] = &entry{p: unsafe.Pointer(&revisionedValue{Revision: v.Revision, Value: v.Value})}
		}
		set.get(maps.String_[i].Map).copy(data)
	}
	// Restore bytes value map.
	for i := range maps.Bytes {
		// Get set.
		set, exist := (*ss)[maps.Bytes[i].Set]
		if !exist {
			logger.Panicf("set is not exist:%s", maps.Bytes[i].Set)
		}
		// Convert BytesValueMap to map[string]*entry
		data := make(map[string]*entry, len(maps.Bytes[i].Data))
		for k, v := range maps.Bytes[i].Data {
			data[k] = &entry{p: unsafe.Pointer(&revisionedValue{Revision: v.Revision, Value: v.Value})}
		}
		set.get(maps.Bytes[i].Map).copy(data)
	}
	// Restore normal value map.
	for i := range maps.Normal {
		// Get set.
		set, exist := (*ss)[maps.Normal[i].Set]
		if !exist {
			logger.Panicf("set is not exist:%s", maps.Bytes[i].Set)
		}
		// Get map.
		m := set.get(maps.Normal[i].Map)
		// Create map[string]struct{uint64,m.vtype.vtype}
		o := reflect.New(reflect.MapOf(reflect.TypeOf(""), buildRevisionedValueType(m.vtype.vtype)))
		// Unmarshal map[string]interface{}
		mustUnmarshal(maps.Normal[i].Data, o.Interface())
		// Convert map[string]struct{uint64,m.vtype.vtype} to map[string]*entry
		data := make(map[string]*entry, o.Elem().Len())
		j := o.Elem().MapRange()
		for j.Next() {
			data[j.Key().String()] = &entry{p: unsafe.Pointer(&revisionedValue{Revision: j.Value().Field(0).Uint(),
				Value: j.Value().Field(1).Interface()})}
		}
		set.get(maps.Normal[i].Map).copy(data)

	}

	return nil
}
