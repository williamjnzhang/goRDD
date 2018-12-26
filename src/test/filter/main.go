package main

import (
	. "rddbase/rdd"
	. "rddbase/functions"
)

func main() {
	a := []int{}
	for i := 0; i < 9; i++ {
		a = append(a, i)
	}
	rdd := BuildRdd(a)
	
	rdd.Foreach(Print_func)

	func1 := func(a RddRow) bool {
		return a.(int) % 2 == 0
	}
	rdd = rdd.Filter(func1)
	rdd.Foreach(Print_func)
}