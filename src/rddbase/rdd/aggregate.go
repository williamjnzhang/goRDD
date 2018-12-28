package rdd

import (
	//"sync"
	"reflect"
)

type OriginType interface{}
type CombinedType interface{}
type SeqOp func(CombinedType, OriginType) CombinedType
type CombOp func(CombinedType, CombinedType) CombinedType
func (rdd Rdd) Aggregate(zeroval CombinedType, sOp SeqOp, cOp CombOp) CombinedType {
	rddsize := rdd.Count()
	nt := getNthread(rddsize, n_thread)
	batchSize := rddsize / nt
	var ochan chan CombinedType = make(chan CombinedType, nt)
	defer close(ochan)
	begin := 0
	for begin < rddsize {
		end := begin + batchSize
		if end > rddsize {
			end = rddsize
		}
		go agg(rdd[begin:end], zeroval, sOp, cOp, ochan)
		begin += batchSize
	}
	ret := zeroval
	for i := 0; i < nt; i++ {
		ret = cOp(ret, <-ochan)
	}
	return ret
}

func agg(rdd Rdd, zeroval CombinedType, sOp SeqOp, cOp CombOp, ochan chan CombinedType) {
	// first round
	rddsize := rdd.Count()
	rrdd := make(Rdd, rddsize, rddsize)
	for idx, row := range rdd {
		rrdd[idx] = sOp(zeroval, row)
	}
	//combine
	ret := rrdd[0]
	for i := 1; i < rddsize; i++ {
		ret = cOp(ret, rrdd[i])
	}
	ochan <- ret
	return
}

// type Keytype interface{}
// type maptype map[Keytype]CombinedType
// type Partition_func func (Keytype) Keytype
// func (rdd Rdd) AggregateByKey(zeroval CombinedType, sOp SeqOp, cOp CombOp, partitioner Partition_func) CombinedType {
// 	// map version
// 	rddsize := rdd.Count()
// 	nt := getNthread(rddsize, n_thread)
// 	batchSize := rddsize / nt
// 	var ochan chan maptype = make(chan maptype, nt)
// 	defer close(ochan)
// 	begin := 0
// 	for begin < rddsize {
// 		end := begin + batchSize
// 		if end > rddsize {
// 			end = rddsize
// 		}
// 		go agg_key(rdd[begin:end], zeroval, sOp, cOp, ochan, partitioner)
// 		begin += batchSize
// 	}

// 	ret := zeroval
// 	for i := 0; i < nt; i++ {
// 		ret = cOp(ret, <-ochan)
// 	}
// 	return ret
// }

// func agg_key(rdd Rdd, zeroval CombinedType, sOp SeqOp, cOp CombOp, ochan chan maptype, partitioner Partition_func) {
// 	v := reflect.ValueOf(rdd[0])
// 	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
// 		panic("The data type of Rdd's row does NOT support AggregateByKey() operation!")
// 	}
// 	if v.Len() != 2 {
// 		panic("The length of Rdd's row is NOT 2!")
// 	}
// 	ret := make(maptype)
// 	for _, row := range rdd {
		
// 	}
// }

type Reduce_func func (OriginType, OriginType) OriginType
func (rdd Rdd) Reduce(rf Reduce_func) OriginType {
	rddsize := rdd.Count()
	nt := getNthread(rddsize, n_thread)
	batchSize := rddsize / nt
	var ochan chan OriginType = make(chan OriginType, nt)
	defer close(ochan)
	begin := 0
	for begin < rddsize {
		end := begin + batchSize
		if end > rddsize {
			end = rddsize
		}
		go red(rdd[begin:end], rf, ochan)
		begin += batchSize
	}
	ret := <- ochan
	for i := 1; i < nt; i++ {
		ret = rf(ret, <-ochan)
	}
	return ret
}

func red(rdd Rdd, rf Reduce_func, ochan chan OriginType) {
	rddsize := rdd.Count()
	ret := rdd[0]
	for i := 1; i < rddsize; i++ {
		ret = rf(ret, rdd[i])
	}
	ochan <- ret
	return
}

type maptype map[Keytype]CombinedType
type CreateCombiner func(OriginType) CombinedType
type MergeValue = SeqOp
type MergeCombiners = CombOp
type Keytype interface{}
type Partition_func func(Keytype) Keytype
func (rdd Rdd) CombineByKey(ccombfunc CreateCombiner, mvalfunc MergeValue, mcombfunc MergeCombiners, pf Partition_func) Rdd {
	// map version
	rddsize := rdd.Count()
	nt := getNthread(rddsize, n_thread)
	batchSize := rddsize / nt
	var ochan chan maptype = make(chan maptype, nt)
	defer close(ochan)
	begin := 0
	for begin < rddsize {
		end := begin + batchSize
		if end > rddsize {
			end = rddsize
		}
		go comb_key(rdd[begin:end], ccombfunc, mvalfunc, mcombfunc, ochan, pf)
		begin += batchSize
	}

	ret := <- ochan
	for i := 1; i < nt; i++ {
		tmp := <- ochan
		for k, v := range tmp {
			if val, ok := ret[k]; ok {
				ret[k] = mcombfunc(val, v)
			} else {
				ret[k] = v
			}
		}
	}
	retrdd := make(Rdd, len(ret), len(ret))
	p := 0
	for k, v := range ret {
		retrdd[p] = Two_tuple{k, v}
		p++
	}
	return retrdd
}


func none_hash(val Keytype) Keytype {
	return val
}

func comb_key(rdd Rdd, ccombfunc CreateCombiner, mvalfunc MergeValue, mcombfunc MergeCombiners, ochan chan maptype, pf Partition_func) {
	v := reflect.ValueOf(rdd[0])
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		panic("The data type of Rdd's row does NOT support XXXByKey() operation!")
	}
	if v.Len() != 2 {
		panic("The number of entries of Rdd's row is NOT 2!")
	}
	if pf == nil {
		pf = none_hash
	}
	ret := make(maptype)
	for _, row := range rdd {
		v = reflect.ValueOf(row)
		key := v.Index(0).Interface()
		val := v.Index(1).Interface()
		hkey := pf(key)
		if cval, ok := ret[hkey]; !ok {
			ret[hkey] = ccombfunc(val)
		} else {
			ret[hkey] = mvalfunc(cval, val)
		}
	}
	ochan <- ret
	return
}