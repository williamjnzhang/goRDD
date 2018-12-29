package functions

import (
	"fmt"
	"rddbase/rdd"
	"reflect"
)

func Print_func(row rdd.RddRow) {
	fmt.Printf("%v\n", row)
}

func Print(row rdd.RddRow) {
	fmt.Printf("%v\n", row)
}

func Add(v1 rdd.OriginType, v2 rdd.OriginType) rdd.OriginType {
	t1 := reflect.TypeOf(v1)
	t2 := reflect.TypeOf(v2)
	if t1 != t2 {
		panic("The types of two entries of Add operation does not same!")
	}
	switch t1.Kind() {
	case reflect.Int:
		return v1.(int) + v2.(int)
	case reflect.Int8:
		return v1.(int8) + v2.(int8)
	case reflect.Int16:
		return v1.(int16) + v2.(int16)
	case reflect.Int32:
		return v1.(int32) + v2.(int32)
	case reflect.Int64:
		return v1.(int64) + v2.(int64)
	case reflect.Uint:
		return v1.(uint) + v2.(uint)
	case reflect.Uint8:
		return v1.(uint8) + v2.(uint8)
	case reflect.Uint16:
		return v1.(uint16) + v2.(uint16)
	case reflect.Uint32:
		return v1.(uint32) + v2.(uint32)
	case reflect.Uint64:
		return v1.(uint64) + v2.(uint64)
	case reflect.Float32:
		return v1.(float32) + v2.(float32)
	case reflect.Float64:
		return v1.(float64) + v2.(float64)
	default:
		panic("The types of two entries does not support Add operation!")
	}
}