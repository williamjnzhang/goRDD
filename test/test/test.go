package main

import (
	"fmt"
	"reflect"
)

func main() {
	a := []int{1,2}
	fmt.Println(f(a))
}

func f(i interface{}) interface{} {
	//a := i.([]interface{})
	v := reflect.ValueOf(i)
	return v.Kind()
}