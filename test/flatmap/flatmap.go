package main

import (
	. "github.com/williamjnzhang/goRDD"
	. "github.com/williamjnzhang/goRDD/functions"
	"strings"
)

func main() {
	a := []string{"a,b,c", "d,e,f,g"}
	// a := []string{}
	originrdd := BuildRddNThread(a, 10)
	fmf := func(row RddRow) SliceArray {
		s := row.(string)
		return strings.Split(s, ",")
	}
	rdd := originrdd.FlatMap(fmf)
	rdd.Foreach(Print)
}