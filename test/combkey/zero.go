package main

import (
	. "github.com/williamjnzhang/goRDD"
	. "github.com/williamjnzhang/goRDD/functions"
)

func main() {
	a := [][2]int{}
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
	originrdd := BuildRddNThread(a, 10)
	rdd := originrdd.CombineByKey(ccombfunc, mvalfunc, mcombfunc, nil)
	rdd.Foreach(Print)
	rdd = originrdd.AggregateByKey([]int{}, mvalfunc, mcombfunc, nil)
	rdd.Foreach(Print)
	rdd = originrdd.ReduceByKey(Add, nil)
	rdd.Foreach(Print)
}
