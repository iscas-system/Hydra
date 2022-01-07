package util

type Queue struct {
	items []interface{}
}

func (q *Queue) New() *Queue {
	q.items = []interface{}{}
	return q
}

func (q *Queue) Push(t interface{}) {
	q.items = append(q.items, t)
}

func (q *Queue) Pop() {
	q.items = q.items[1:len(q.items)]
}

func (q *Queue) Front() interface{} {
	item := q.items[0]
	return item
}

func (q *Queue) Empty() bool {
	return len(q.items) == 0
}

func (q *Queue) Size() int {
	return len(q.items)
}
