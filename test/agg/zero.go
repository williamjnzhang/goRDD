package main

import (
	. "github.com/williamjnzhang/goRDD"
	. "github.com/williamjnzhang/goRDD/functions"
	"fmt"
)

func main() {
	a := [][]int{}
	rdd := BuildRddNThread(a, 10)
	
	rdd.Foreach(Print_func)

	seqOp := func(v1 CombinedType, v2 OriginType) CombinedType {
		val1 := v1.(int)
		val2 := v2.([]int)
		return val1 + val2[1]
	}
	combOp := func(v1 CombinedType, v2 CombinedType) CombinedType {
		val1 := v1.(int)
		val2 := v2.(int)
		return val1 + val2
	}
	res := rdd.Aggregate(0, seqOp, combOp)
	fmt.Println(res)
}
