package main

import (
	. "rddbase/rdd"
	. "rddbase/functions"
	"strings"
)

func main() {
	a := []string{"a,b,c", "d,e,f,g"}
	originrdd := BuildRddNThread(a, 10)
	fmf := func(row RddRow) SliceArray {
		s := row.(string)
		return strings.Split(s, ",")
	}
	rdd := originrdd.FlatMap(fmf)
	rdd.Foreach(Print)
}