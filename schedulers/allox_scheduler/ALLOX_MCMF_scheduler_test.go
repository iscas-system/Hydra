package allox_scheduler

import (
	"fmt"
	"strconv"
	"testing"
)

func TestSolver(t *testing.T) {
	g := NewGraph()
	source := NewNode("source", "source")
	sink := NewNode("sink", "sink")
	g.AddSource(source)
	g.AddSink(sink)

	jobNum := 3
	gpuNum := 2

	// Matrix P in the paper
	timeMatrix := make([][]float64, jobNum)
	for i := 0; i < jobNum; i++ {
		timeMatrix[i] = make([]float64, gpuNum)
	}
	timeMatrix[0][0] = 3
	timeMatrix[0][1] = 4
	timeMatrix[1][0] = 4
	timeMatrix[1][1] = 6
	timeMatrix[2][0] = 5
	timeMatrix[2][1] = 10

	jobNodes := make([]*Node, jobNum)
	gpuSlotNodes := make([]*Node, jobNum*gpuNum)

	for i := 0; i < jobNum; i++ {
		jobNodes[i] = NewNode("job"+strconv.Itoa(i), "job")
	}

	for i := 0; i < jobNum; i++ {
		for j := 0; j < gpuNum; j++ {
			// gpu-j 's slot i (the i th job on gpu j)
			gpuSlotNodes[i*gpuNum+j] = NewNode("gpu"+strconv.Itoa(j)+"-"+"slot"+strconv.Itoa(i), "gpu")
		}
	}

	weights := make([][]float64, jobNum)
	for i := 0; i < jobNum; i++ {
		weights[i] = make([]float64, jobNum*gpuNum)
		for j := 0; j < jobNum; j++ {
			for k := 0; k < gpuNum; k++ {
				weights[i][j*gpuNum+k] = timeMatrix[i][k] * float64(j+1)
			}
		}
	}

	for _, node := range jobNodes {
		g.AddNode(node)
		g.AddEdge(source, node, 1., 0)
	}

	for _, node := range gpuSlotNodes {
		g.AddNode(node)
		g.AddEdge(node, sink, 1., 0)
	}

	for i, job := range jobNodes {
		for j, gpuSlot := range gpuSlotNodes {
			g.AddEdge(job, gpuSlot, 1., weights[i][j])
		}
	}
	solver := NewMCMFSolver(g)
	solver.Solve()

	fmt.Println("Minimum JCT:", solver.minCost)
	fmt.Println(solver.maxFlow)
	fmt.Println("Scheduling result:", solver.GetSchedulingResult())
}
