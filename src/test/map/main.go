package main

import (
	. "rddbase/rdd"
	. "rddbase/functions"
)

func main() {
	a := []int{1,2,3,4}
	rdd := BuildRdd(a)
	
	rdd.Foreach(Print_func)

	func1 := func(a RddRow) RddRow {
		return 2 * a.(int)
	}
	rdd = rdd.Map(func1)
	rdd.Foreach(Print_func)
}