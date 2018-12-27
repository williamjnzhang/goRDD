package main

import (
	. "rddbase/rdd"
	. "rddbase/functions"
	"fmt"
)

func main() {
	a := []int{}
	for i := 0; i < 100; i++ {
		a = append(a, i)
	}
	rdd := BuildRddNThread(a, 10)
	
	rdd.Foreach(Print_func)
	rf := func(v1 OriginType, v2 OriginType) OriginType {
		val1 := v1.(int)
		val2 := v2.(int)
		return val1 + val2
	}
	res := rdd.Reduce(rf)
	fmt.Println(res)
}