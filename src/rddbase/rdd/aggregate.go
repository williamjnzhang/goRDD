package rdd

import (
	//"sync"
)

type OriginType interface{}
type CombinedType interface{}
type seqOp func(CombinedType, OriginType) CombinedType
type combOp func(CombinedType, CombinedType) CombinedType
func (rdd Rdd) Aggregate(zeroval CombinedType, sOp seqOp, cOp combOp) CombinedType {
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

func agg(rdd Rdd, zeroval CombinedType, sOp seqOp, cOp combOp, ochan chan CombinedType) {
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