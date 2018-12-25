package rdd

import (
	"fmt"
	"reflect"
	"encoding/gob"
	"bytes"
)

type RddRow interface{}

type Rdd []RddRow

type Two_tuple [2]interface{}

type Three_tuple [3]interface{}

func BuildRdd(data interface{}) Rdd {
	t := reflect.TypeOf(data)
	switch t.Kind() {
	case reflect.Ptr:
		return BuildRdd(reflect.ValueOf(data).Elem())
	case reflect.Slice:
		v := reflect.ValueOf(data)
		size := v.Len()
		rdd := make(Rdd, size, size)
		for i := 0; i < size; i++ {
			rdd[i] = v.Index(i).Interface()
		}
		return rdd
	case reflect.Map:
		v := reflect.ValueOf(data)
		size := v.Len()
		rdd := make(Rdd, size, size)
		mkeys := v.MapKeys()
		for i := 0; i < size; i++ {
			mkey := mkeys[i]
			mval := v.MapIndex(mkey)
			tow_tup := Two_tuple{mkey.Interface(), mval.Interface()}
			rdd[i] = tow_tup
		}
		return rdd
	default:
		rdd := make(Rdd, 1, 1)
		rdd[0] = data
		return rdd
	}
}

func deepCopy(src, dst interface{}) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func (rdd Rdd) Count() int {
	return len(rdd)
}

type foreach_func func(RddRow)
func (rdd Rdd) Foreach(ff foreach_func) {
	for _, row := range rdd {
		ff(row)
	}
}

type map_func func(RddRow) RddRow
func (rdd Rdd) Map(mf map_func) Rdd {
	rddsize := rdd.Count()
	newRdd := make(Rdd, rddsize, rddsize)
	for idx, row := range rdd {
		newRdd[idx] = mf(row)
	}
	return newRdd
}

