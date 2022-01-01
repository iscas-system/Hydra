package allox_scheduler

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestSolver2(t *testing.T) {
	file, _ := os.Open("/Users/yangchen/Projects/Graduate/DES-go/cases/case_3600.csv")
	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()
	records = records[1:]
	records = records[:600]
	//fmt.Println(records[len(records) - 1])

	g := NewGraph()
	source := NewNode("source", "source")
	sink := NewNode("sink", "sink")
	g.AddSource(source)
	g.AddSink(sink)

	jobNum := 600
	gpuNum := 60

	// Matrix P in the paper
	timeMatrix := make([][]float64, jobNum)
	for i := 0; i < jobNum; i++ {
		timeMatrix[i] = make([]float64, gpuNum)
	}
	for i := 0; i < jobNum; i++ {
		for j := 0; j < gpuNum; j++ {
			if j < 10 {
				t, _ := strconv.Atoi(records[i][2])
				timeMatrix[i][j] = float64(t)
			} else if j >= 10 && j < 30 {
				t, _ := strconv.Atoi(records[i][3])
				timeMatrix[i][j] = float64(t)
			} else {
				t, _ := strconv.Atoi(records[i][4])
				timeMatrix[i][j] = float64(t)

			}
		}
	}
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
	fmt.Println("Scheduling result:", solver.GetSchedulingResult())

}
