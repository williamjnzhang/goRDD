package main

import (
	"rddbase/rdd"
	"rddbase/functions"
)

func main() {
	a := [][2]int{[2]int{1,1}, [2]int{1,2}, [2]int{2,3}, [2]int{3,4}, [2]int{3,5}, [2]int{4,6}, [2]int{5,7}, [2]int{5,8}}
	b := [][2]int{[2]int{2,1}, [2]int{2,2}, [2]int{4,3}, [2]int{5,4}, [2]int{5,5}, [2]int{5,6}, [2]int{1,7}}

	x := rdd.BuildRddNThread(a, 10)
	y := rdd.BuildRddNThread(b, 10)
	
	less := func(i, j rdd.RddRow) bool {
		xi := i.([2]int)
		yj := j.([2]int)
		return xi[0] < yj[0]
	}

	equal := func(i, j rdd.RddRow) bool {
		xi := i.([2]int)
		yj := j.([2]int)
		return xi[0] == yj[0]
	}
	rdd := x.Join(y, less, equal)
	rdd.Foreach(functions.Print)
}