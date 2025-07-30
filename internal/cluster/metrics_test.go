package cluster

import (
	"fmt"
	"runtime"
	"testing"
)

func TestSystemUtil(t *testing.T) {

	getMemory()

	fmt.Println(runtime.NumCPU())

}
