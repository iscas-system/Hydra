package util

type Sorter struct {
	LenFunc  func() int
	LessFunc func(i, j int) bool
	SwapFunc func(i, j int)
}

func (s Sorter) Len() int {
	return s.LenFunc()
}

func (s Sorter) Less(i, j int) bool {
	return s.LessFunc(i, j)
}

func (s Sorter) Swap(i, j int) {
	s.SwapFunc(i, j)
}

type HeapSorter struct {
	LenFunc  func() int
	LessFunc func(i, j int) bool
	SwapFunc func(i, j int)
	PushFunc func(x interface{})
	PopFunc  func() interface{}
}

func (b HeapSorter) Len() int {
	return b.LenFunc()
}

func (b HeapSorter) Less(i, j int) bool {
	return b.LessFunc(i, j)
}

func (b HeapSorter) Swap(i, j int) {
	b.SwapFunc(i, j)
}

func (b HeapSorter) Push(x interface{}) {
	b.PushFunc(x)
}

func (b HeapSorter) Pop() interface{} {
	return b.PopFunc()
}
