package rdd

import (
	"fmt"
	"reflect"
	"encoding/gob"
	"bytes"
	"sync"
)

type RddRow interface{}

type Rdd []RddRow

type Two_tuple [2]interface{}

type Three_tuple [3]interface{}

var n_thread int

const (
	min_parallel_size = 10
)

func getNthread(rddsize, n_thread int) int {
	batchSize := rddsize / n_thread
	if batchSize < min_parallel_size {
		batchSize = min_parallel_size
	}
	
	ret := rddsize / batchSize
	if ret < 1 {
		ret = 1
	}
	return ret
}

func BuildRdd(data interface{}) Rdd {
	return BuildRddNThread(data, 1)
}

func BuildRddNThread(data interface{}, num_thread int) Rdd {
	n_thread = num_thread
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
	nt := getNthread(rddsize, n_thread)
	batchSize := rddsize / nt
	var wg sync.WaitGroup
	begin := 0
	for begin < rddsize {
		end := begin + batchSize
		if end > rddsize {
			end = rddsize
		}
		wg.Add(1)
		go func(b, e int) {
			defer wg.Done()
			for i := b; i < e; i++ {
				newRdd[i] = mf(rdd[i])
			}
		}(begin, end)
		begin += batchSize
	}
	wg.Wait()

	return newRdd
}

type filter_func func(RddRow) bool
func (rdd Rdd) Filter(ff filter_func) Rdd {
	rddsize := rdd.Count()
	nt := getNthread(rddsize, n_thread)
	batchSize := rddsize / nt
	var rddchan chan Rdd = make(chan Rdd, nt)
	begin := 0
	for begin < rddsize {
		end := begin + batchSize
		if end > rddsize {
			end = rddsize
		}
		go func(b, e int) {
			newRdd := make(Rdd, 0)
			for i := b; i < e; i++ {
				if ff(rdd[i]) {
					newRdd = append(newRdd, rdd[i])
				}
			}
			rddchan <- newRdd
		}(begin, end)
		begin += batchSize
	}
	newRdd := make(Rdd, 0)
	for i := 0; i < nt; i++ {
		nr := <- rddchan
		newRdd = append(newRdd, nr...)
	}
	return newRdd
}

func (rdd Rdd) First() interface{} {
	return rdd[0]
}

type OriginType interface{}
type CombinedType interface{}
type seqOp func(CombinedType, OriginType) CombinedType
type combOp func(CombinedType, CombinedType) CombinedType
func (rdd Rdd) Aggregate(zeroval CombinedType, sOp seqOp, cOp combOp) Rdd {
	return nil
}