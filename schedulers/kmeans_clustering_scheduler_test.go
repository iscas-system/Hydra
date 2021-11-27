package schedulers

import (
	"testing"
)

func Test1(t *testing.T) {
	nodes := make([]int, 0)
	f := func() {
		nodes = append(nodes, 10)
	}
	f()
	t.Logf("%+v", nodes)
}
