package simulator

import (
	"testing"
)

func TestJobExecutionDetail_AddExecutionRange(t *testing.T) {
	initDataSource("/Users/purchaser/go/src/DES-go/cases/case1.csv", nil)
	j := NewJob("job1")
	j.executionDetail = newJobExecutionDetail("job1")
	gpu := NewGPU(1, "V100")
	j.executionDetail.addExecutionRange(gpu, newTimeRange(0, 1))
	// t.Logf(util.PrettyPretty(j))

	m := make(map[*GPU]*Job)
	m[gpu] = j
	//t.Log(util.PrettyF("pretty\n%# v\n", m))
}

func TestNewJob(t *testing.T) {

}
