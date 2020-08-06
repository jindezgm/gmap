/*
 * @Author: jinde.zgm
 * @Date: 2020-07-28 21:12:04
 * @Descripttion:
 */

package gmap

import (
	"reflect"
	"sync"
)

// setValueTypes is map[reflect.TypeOf(value).String()]*typedValue.
// setValueTypes is global thread safe map, it is used for grpc to create value object.
var valueTypes sync.Map

// valueType define functions of value type.
type valueType struct {
	vtype  reflect.Type                       // Value type, reflect.TypeOf(value)
	ptype  reflect.Type                       // Converted protobuf value type
	wrap   func(value interface{}) ProtoValue // Convert value to protobuf object.
	unwrap func(value ProtoValue) interface{} // Convert protobuf object to value
}

// newValue create value object.
func (vt *valueType) newValue() interface{} {
	return reflect.New(vt.vtype.Elem()).Interface()
}

// newProtoValue create protobufable value object.
func (vt *valueType) newProtoValue() ProtoValue {
	if nil != vt.ptype {
		return reflect.New(vt.ptype.Elem()).Interface().(ProtoValue)
	}

	return reflect.New(vt.vtype.Elem()).Interface().(ProtoValue)
}

// ProtoValue defines the necessary interfaces for protobuf serialization.
// Check value object is a protobuf object or not by the type assert.
type ProtoValue interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Size() int
	MarshalToSizedBuffer([]byte) (int, error)
}

// RevisionedProtoValue is revisioned protobufable value.
type RevisionedProtoValue struct {
	Set      string
	Revision uint64
	Value    ProtoValue
}

// revisionedValue is reversioned values, there is an incremental reversion of value in gmap.
type revisionedValue struct {
	Revision uint64
	Value    interface{}
}

// newValueType create valueType by value type.
func newValueType(typ reflect.Type) *valueType {
	vt := &valueType{vtype: typ}
	switch {
	// Value type is protobuf object.
	case typ.Kind() == reflect.Ptr && typ.Elem().Implements(reflect.TypeOf((*ProtoValue)(nil)).Elem()):
		vt.wrap = func(value interface{}) ProtoValue { return value.(ProtoValue) }
		vt.unwrap = func(value ProtoValue) interface{} { return value }
	// Value type is string
	case typ.Kind() == reflect.String:
		vt.ptype = reflect.TypeOf(&StringValue{})
		vt.wrap = func(value interface{}) ProtoValue { return &StringValue{Value: value.(string)} }
		vt.unwrap = func(value ProtoValue) interface{} { return value.(*StringValue).Value }
	// Value type is []byte
	case typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8:
		vt.ptype = reflect.TypeOf(&BytesValue{})
		vt.wrap = func(value interface{}) ProtoValue { return &BytesValue{Value: value.([]byte)} }
		vt.unwrap = func(value ProtoValue) interface{} { return value.(*BytesValue).Value }
	// Value type is struct pointer.
	default:
		if typ.Kind() != reflect.Ptr {
			logger.Panicf("All types must be pointers to structs.")
		}
		// If value type is not protobuf, string or [] byte, only convert value to JSON and then treat it as []byte
		vt.ptype = reflect.TypeOf(&BytesValue{})
		vt.wrap = func(value interface{}) ProtoValue { return &BytesValue{Value: mustMarshal(value)} }
		vt.unwrap = func(value ProtoValue) interface{} {
			v := reflect.New(vt.vtype.Elem()).Interface()
			mustUnmarshal(value.(*BytesValue).Value, v)
			return v
		}
	}

	return vt
}

// newValue create value object which type in the specific set.
func newValue(set string) interface{} {
	vt, exist := valueTypes.Load(set)
	if !exist {
		logger.Panicf("not exist value type:%v", set)
	}

	return vt.(*valueType).newValue()
}

// newProtoValue create protobufable value object which type in specific set.
func newProtoValue(set string) ProtoValue {
	vt, exist := valueTypes.Load(set)
	if !exist {
		logger.Panicf("not exist value type:%v", set)
	}

	return vt.(*valueType).newProtoValue()
}

// buildRevisionedValueType build specific revisioned value type.
func buildRevisionedValueType(typ reflect.Type) reflect.Type {
	fields := []reflect.StructField{
		{Name: "Revision", Type: reflect.TypeOf(uint64(0))},
		{Name: "Value", Type: typ},
	}

	return reflect.StructOf(fields)
}
