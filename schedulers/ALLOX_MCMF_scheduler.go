package schedulers

import (
	"DES-go/simulator"
	"DES-go/util"
	"math"
)


// In Allox, the author build the scheduling problem as a min-cost bipartite graph matching problem


// In graph theory, a min-cost bipartite graph matching problem can be transformed into an equal min cost max flow (MCMF) problem
// Here wo use spfa algorithm to find augmenting path until we can't find more to solve MCMF problem

// BTW, another common way to solve the bipartite graph max match problem is KM algorithm, but it's a bit more complicated to implement.

type AlloxScheduler struct {
	cluster *simulator.Cluster


}
type Node struct {
	name string
}
func NewNode(name string) *Node {
	return &Node{name: name}

}

type Edge struct {
	name string
	from, to *Node
	// 边的容量，随求解的过程实时更新
	// 准确来说边的容量应该是一个定值，这个准确的说应该是边上的流量
	// 为方便处理没有进行区分
	capacity float64
	// 边的权重，指边上单位容量上对应的费用
	weight float64
}

func NewEdge(from, to *Node, capacity, weight float64) *Edge {
	edge := &Edge{
		from: from,
		to: to,
		capacity: capacity,
		weight: weight,
	}
	edge.name = from.name + "->" + to.name
	return edge
}

type Graph struct {
	// 源点和汇点
	source, sink *Node

	// 所有节点
	nodes []*Node

	// 每个节点的所有出边
	outs map[Node][]*Edge

	// 每个边对应的反向边
	// Edge类型在求解过程中，capacity会发生改变，不能使用Edge作为key, 采用Edge的Name作为key
	reverse map[string]*Edge


}
func NewGraph() *Graph {
	return &Graph{
		nodes: make([]*Node, 0),
		outs:  make(map[Node][]*Edge),
		reverse: make(map[string]*Edge),
	}
}

func (g *Graph) AddSource(node *Node) {
	g.source = node
	g.AddNode(node)
}

func (g *Graph) AddSink(node *Node) {
	g.sink = node
	g.AddNode(node)


}

func (g *Graph) AddNode(node *Node) {
	g.nodes = append(g.nodes, node)
}

func (g *Graph) AddEdge(from, to *Node, capacity, weight float64) {
	e1 := NewEdge(from ,to , capacity, weight)
	e2 := NewEdge(to, from, 0, -weight)

	g.outs[*from] = append(g.outs[*from], e1)
	g.outs[*to] = append(g.outs[*to], e2)

	g.reverse[e1.name] = e2
	g.reverse[e2.name] = e1
}


type MCMFSolver struct {

	graph *Graph
	result map[Node]*Node

	maxFlow float64
	minCost float64

	flow map[Node]float64
	pre map[Node]*Node
	l map[Node]*Edge

	q util.Queue

	distance map[Node]float64
	visit map[Node]bool
}

func NewMCMFSolver(graph *Graph) *MCMFSolver {
	solver := &MCMFSolver{
		graph: graph,
		result: make(map[Node]*Node),
		flow: make(map[Node]float64),
		pre: make(map[Node]*Node),
		l: make(map[Node]*Edge),
		distance: make(map[Node]float64),
		visit: make(map[Node]bool),
	}
	solver.q.New()
	return solver
}

func (solver *MCMFSolver) Solve() {
	for solver.spfa() {
		solver.maxFlow += solver.flow[*solver.graph.sink]
		solver.minCost += solver.distance[*solver.graph.sink] * solver.flow[*solver.graph.sink]
		for node := *solver.graph.sink; node != *solver.graph.source; node = *solver.pre[node] {
			edge := solver.l[node]
			edge.capacity -= solver.flow[*solver.graph.sink]
			solver.graph.reverse[edge.name].capacity += solver.flow[*solver.graph.sink]
		}
	}
}

func (solver *MCMFSolver) spfa() bool {

	for _, node := range solver.graph.nodes {
		solver.distance[*node] = math.Inf(1)
		solver.visit[*node] = false
	}
	solver.visit[*solver.graph.source] = true
	solver.distance[*solver.graph.source] = 0
	solver.pre[*solver.graph.sink] = nil

	solver.flow[*solver.graph.source] = math.Inf(1)
	solver.q.Push(*solver.graph.source)
	for !solver.q.Empty() {
		u := solver.q.Front().(Node)
		solver.q.Pop()
		solver.visit[u] = false

		for _, e := range solver.graph.outs[u] {
			v := *e.to
			w := e.weight
			f := e.capacity

			if f > 0 && solver.distance[v] > solver.distance[u] + w {
				solver.distance[v] = solver.distance[u] + w
				solver.pre[v] = &u
				solver.l[v] = e
				solver.flow[v] = math.Min(solver.flow[u], f)

				if !solver.visit[v] {
					solver.q.Push(v)
					solver.visit[v] = true
				}
			}
		}
	}

	return solver.pre[*solver.graph.sink] != nil
}