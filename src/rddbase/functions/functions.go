package functions

import (
	"fmt"
	"rddbase/rdd"
)

func Print_func(row rdd.RddRow) {
	fmt.Printf("%v\n", row)
}