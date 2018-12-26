package main

import (
	. "rddbase/rdd"
	. "rddbase/functions"
)

func main() {
	a := []int{}
	for i := 0; i < 100; i++ {
		a = append(a, i)
	}
	rdd := BuildRdd(a)
	
	rdd.Foreach(Print_func)

	func1 := func(a RddRow) RddRow {
		return 2 * a.(int)
	}
	rdd = rdd.Map(func1)
	rdd.Foreach(Print_func)
}