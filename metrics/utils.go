package metrics

import (
	"DES-go/schedulers/types"
	"DES-go/util"
)

// avgJCT 计算一批任务的平均JCT
func avgJCT(jobs []types.Job) float64 {
	if len(jobs) == 0 {
		return 0
	}
	sumJCT := types.Time(0.)
	for _, job := range jobs {
		sumJCT += job.JCT()
	}
	return float64(sumJCT / types.Time(len(jobs)))
}

// violation 获取违约的任务，以及平均DDL违约时间
func violation(jobs []types.Job) ([]types.Job, float64) {
	if len(jobs) == 0 {
		return nil, 0
	}
	sumViolationDelay := types.Duration(0.)
	violatedJobs := make([]types.Job, 0)
	for _, job := range jobs {
		violated, violationDelay := job.Violation()
		if violated {
			sumViolationDelay += violationDelay
			violatedJobs = append(violatedJobs, job)
		}
	}
	return violatedJobs, float64(sumViolationDelay) / float64(len(jobs))
}

func avgQueuingDelay(jobs []types.Job) float64 {
	if len(jobs) == 0 {
		return 0
	}
	is := make([]interface{}, 0, len(jobs))
	for _, job := range jobs {
		is = append(is, job)
	}
	return util.AvgFloat64(func(i interface{}) float64 {
		j := i.(types.Job)
		return float64(j.QueueDelay())
	}, is...)
}

func packJobs(jobs []types.Job) []*Job {
	res := make([]*Job, 0, len(jobs))
	for _, job := range jobs {
		res = append(res, packJob(job))
	}
	return res
}

func packJob(job types.Job) *Job {
	violated, violatedDuration := job.Violation()
	resExecutionRanges := make([]*JobExecutionRange, 0)
	for gpu, executionRanges := range job.ExecutionDetail().ExecutionRanges() {
		for _, er := range executionRanges {
			timeRange := er.TimeRange()
			resExecutionRanges = append(resExecutionRanges, &JobExecutionRange{
				GPU:       string(gpu.Type()),
				StartTime: float64(timeRange.Start()),
				End:       float64(timeRange.End()),
				Runtime:   float64(timeRange.Runtime()),
			})
		}
	}
	return &Job{
		Name:               string(job.JobName()),
		SubmitTime:         float64(job.JobMeta().SubmitTime()),
		FinishedTime:       float64(job.FinishExecutionTime()),
		JCT:                float64(job.JCT()),
		DDL:                float64(job.JobMeta().DDL()),
		Violated:           violated,
		ViolatedDuration:   float64(violatedDuration),
		QueueDelayDuration: float64(job.QueueDelay()),
		ExecutionRanges:    resExecutionRanges,
	}
}
