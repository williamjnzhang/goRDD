package main

import (
	. "rddbase/rdd"
	. "rddbase/functions"
)

func main() {
	a := [][2]int{[2]int{1,2}, [2]int{1,3}, [2]int{2,2}}
	ccombfunc := func(o OriginType) CombinedType {
		return []int{o.(int)}
	}
	mvalfunc := func(c CombinedType, o OriginType) CombinedType {
		cv := c.([]int)
		ov := o.(int)
		return append(cv, ov)
	}
	mcombfunc := func(c1 CombinedType, c2 CombinedType) CombinedType {
		c1v := c1.([]int)
		c2v := c2.([]int)
		return append(c1v, c2v...)
	}
	rdd := BuildRddNThread(a, 10)
	rdd = rdd.CombineByKey(ccombfunc, mvalfunc, mcombfunc, nil)
	rdd.Foreach(Print_func)
}