package simulator

import (
	"DES-go/schedulers/types"
	"DES-go/util"
)

// AvgJCT 计算一批任务的平均JCT
func AvgJCT(jobs []*Job) types.Time {
	if len(jobs) == 0 {
		return 0
	}
	sumJCT := types.Time(0.)
	for _, job := range jobs {
		sumJCT += job.JCT()
	}
	return sumJCT / types.Time(len(jobs))
}

// MetricViolation 计算一批任务的违约个数，以及平均DDL违约时间
func MetricViolation(jobs []*Job) (int, types.Duration) {
	if len(jobs) == 0 {
		return 0, 0
	}
	sumViolationDelay := types.Duration(0.)
	violatedCount := 0
	for _, job := range jobs {
		violated, violationDelay := job.Violation()
		if violated {
			sumViolationDelay += violationDelay
			violatedCount += 1
		}
	}
	return violatedCount, sumViolationDelay / types.Duration(len(jobs))
}

func AvgQueuingDelay(jobs []*Job) types.Duration {
	is := make([]interface{}, 0, len(jobs))
	for _, job := range jobs {
		is = append(is, job)
	}
	return types.Duration(util.AvgFloat64(func(i interface{}) float64 {
		j := i.(*Job)
		return float64(j.QueueDelay())
	}, is...))
}
