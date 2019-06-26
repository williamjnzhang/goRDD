package main

import (
	. "github.com/williamjnzhang/goRDD"
	. "github.com/williamjnzhang/goRDD/functions"
)

func main() {
	a := []int{}
	rdd := BuildRddNThread(a, 10)
	
	rdd.Foreach(Print_func)

	func1 := func(a RddRow) RddRow {
		return 2 * a.(int)
	}
	rdd = rdd.Map(func1)
	rdd.Foreach(Print_func)
}
