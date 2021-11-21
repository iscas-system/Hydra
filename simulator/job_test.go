package simulator

import (
	"DES-go/util"
	"testing"
)

func TestJobExecutionDetail_AddExecutionRange(t *testing.T) {
	InitDataSource("/Users/purchaser/go/src/DES-go/cases/case1.csv")
	j := NewJob("job1")
	j.executionDetail = NewJobExecutionDetail("job1")
	gpu := NewGPU(1, "V100")
	j.executionDetail.AddExecutionRange(gpu, NewTimeRange(0, 1))
	// t.Logf(util.PrettyPretty(j))

	m := make(map[*GPU]*Job)
	m[gpu] = j
	t.Log(util.PrettyF("pretty\n%# v\n", m))
}

func TestNewJob(t *testing.T) {

}