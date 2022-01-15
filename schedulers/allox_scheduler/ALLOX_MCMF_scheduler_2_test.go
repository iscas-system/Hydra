package allox_scheduler

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
)

func TestSolver2(t *testing.T) {
	//file, _ := os.Open("/Users/yangchen/Projects/Graduate/DES-go/cases/case_200_all_2.csv")
	file, _ := os.Open("/Users/purchaser/go/src/DES-go/cases/case_200_all_2.csv")
	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()
	records = records[1:]
	//fmt.Println(records[len(records) - 1])
	g := NewGraph()
	source := NewNode("source", "source")
	sink := NewNode("sink", "sink")
	g.AddSource(source)
	g.AddSink(sink)

	jobNum := 200
	gpuNum := 20

	gpuSlice := make([]string, 0, gpuNum)
	for i := 0; i < gpuNum; i++ {
		gpuSlice = append(gpuSlice, fmt.Sprintf("gpu-%d", i))
	}
	jobsSlice := make([]string, 0, jobNum)
	for i := 0; i < jobNum; i++ {
		jobsSlice = append(jobsSlice, fmt.Sprintf("job-%d", i))
	}
	// Matrix P in the paper
	timeMatrix := make([][]float64, jobNum)
	for i := 0; i < jobNum; i++ {
		timeMatrix[i] = make([]float64, gpuNum)
	}
	for i := 0; i < jobNum; i++ {
		for j := 0; j < gpuNum; j++ {
			if j < 4 {
				t, _ := strconv.ParseFloat(records[i][4], 64)
				timeMatrix[i][j] = t
			} else if j >= 4 && j < 12 {
				t, _ := strconv.ParseFloat(records[i][5], 64)
				timeMatrix[i][j] = t
			} else {
				t, _ := strconv.ParseFloat(records[i][6], 64)
				timeMatrix[i][j] = t

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

	scheduleResult := solver.GetSchedulingResult()
	fmt.Println("Minimum JCT:", solver.minCost)
	fmt.Println(solver.maxFlow)
	fmt.Println("Scheduling result:", scheduleResult)
	gpuSlotReg := regexp.MustCompile(`^gpu([0-9]+)-slot([0-9]+)$`)
	jobIdxReg := regexp.MustCompile(`^job([0-9]+)$`)
	gpu2slot2job := make(map[string]map[int]string)
	for _, gpu := range gpuSlice {
		gpu2slot2job[gpu] = make(map[int]string)
	}
	for jobStr, gpuSlotStr := range scheduleResult {
		jobStrMatches := jobIdxReg.FindStringSubmatch(jobStr)
		jobIdx, _ := strconv.Atoi(jobStrMatches[1])
		gpuSlotMatches := gpuSlotReg.FindStringSubmatch(gpuSlotStr)
		gpuIdx, _ := strconv.Atoi(gpuSlotMatches[1])
		gpuSlot, _ := strconv.Atoi(gpuSlotMatches[2])
		gpu2slot2job[gpuSlice[gpuIdx]][gpuSlot] = jobsSlice[jobIdx]
	}

	fmt.Println("Scheduling gpu2slot2job:", gpu2slot2job)

}
