package rdd

import (
	"reflect"
	"sort"
	"sync"
)

func checkKVRdd(rdd Rdd) {
	v := reflect.ValueOf(rdd[0])
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		panic("The data type of Rdd's row does NOT support XXXByKey() operation!")
	}
	if v.Len() != 2 {
		panic("The number of entries of Rdd's row is NOT 2!")
	}
	return
}


type SortLess func(i, j RddRow) bool
type SortEqual func(i, j RddRow) bool
func (x Rdd) Join(y Rdd, sortless SortLess, sortequal SortEqual) Rdd {
	retrdd := make(Rdd, 0)
	if len(x) <= 0 || len(y) <= 0 {
		return retrdd
	}
	// check x and y
	checkKVRdd(x)
	checkKVRdd(y)
	
	xless := func (i, j int) bool {
		return sortless(x[i], x[j])
	}
	yless := func (i, j int) bool {
		return sortless(y[i], y[j])
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sort.Slice(x, xless)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		sort.Slice(y, yless)
	}()
	wg.Wait()
	ret := make(Rdd, 0)
	i := 0
	j := 0
	j1 := -1
	for true {
		if sortequal(x[i], y[j]) {
			if j1 < 0 {
				j1 = j
			}
			// v = reflect.ValueOf(row)
			// key := v.Index(0).Interface()
			// val := v.Index(1).Interface()
			xi := reflect.ValueOf(x[i])
			xkey := xi.Index(0).Interface()
			xval := xi.Index(1).Interface()
			yj := reflect.ValueOf(y[j])
			// ykey := yj.Index(0).Interface()
			yval := yj.Index(1).Interface()
			valtup := Two_tuple{xval, yval}

			ret = append(ret, Two_tuple{xkey, valtup})

			j++
		} else if sortless(x[i], y[j]) {
			if j1 >= 0 {
				i ++
				j = j1
			} else {
				i ++
			}
		} else {
			j1 = -1
			j ++
		}
		if j >= len(y) {
			if j1 >= 0 {
				j = j1
				i ++
			} else {
				break
			}
		}
		if i >= len(x) {
			break
		}
	}
	return ret
}