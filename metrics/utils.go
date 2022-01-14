package metrics

import (
	"DES-go/schedulers/types"
	"DES-go/util"
)

// avgJCT 计算一批任务的平均JCT
func avgJCT(jobs []types.Job) types.Time {
	if len(jobs) == 0 {
		return 0
	}
	sumJCT := types.Time(0.)
	for _, job := range jobs {
		sumJCT += job.JCT()
	}
	return sumJCT / types.Time(len(jobs))
}

// violation 计算一批任务的违约个数，以及平均DDL违约时间
func violation(jobs []types.Job) (int, types.Duration) {
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

func avgQueuingDelay(jobs []types.Job) types.Duration {
	is := make([]interface{}, 0, len(jobs))
	for _, job := range jobs {
		is = append(is, job)
	}
	return types.Duration(util.AvgFloat64(func(i interface{}) float64 {
		j := i.(types.Job)
		return float64(j.QueueDelay())
	}, is...))
}
