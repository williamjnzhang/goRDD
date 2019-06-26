package main

import (
	. "github.com/williamjnzhang/goRDD"
	. "github.com/williamjnzhang/goRDD/functions"
)

func main() {
	a := []int{}
	rdd := BuildRddNThread(a, 10)
	
	rdd.Foreach(Print_func)

	func1 := func(a RddRow) bool {
		return a.(int) % 2 == 0
	}
	rdd = rdd.Filter(func1)
	rdd.Foreach(Print_func)
}
