package simulator

import (
	"DES-go/schedulers/types"
	"fmt"
	"math"
)

type GPUJobQueue struct {
	gpu  *GPU
	jobs []*Job
}

func (q *GPUJobQueue) GPU() types.GPU {
	return q.gpu
}

func (q *GPUJobQueue) Jobs() []types.Job {
	jobs := make([]types.Job, 0, len(q.jobs))
	for _, j := range q.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (q *GPUJobQueue) SetJobs(jobs ...types.Job) {
	res := make([]*Job, 0, len(jobs))
	for _, j := range jobs {
		res = append(res, j.(*Job))
	}
	q.jobs = res
	// those jobs not on the first rank cannot have 'running' status
	for i := 1; i < len(q.jobs); i++ {
		q.jobs[i].setNotRunning()
	}
}

func (q *GPUJobQueue) ClearQueue() []types.Job {
	for _, job := range q.jobs {
		job.setNotRunning()
	}
	res := q.Jobs()
	q.jobs = []*Job{}
	return res
}

func NewGPUJobQueue(gpu types.GPU) *GPUJobQueue {
	return &GPUJobQueue{
		gpu:  gpu.(*GPU),
		jobs: make([]*Job, 0),
	}
}

func (q *GPUJobQueue) passDuration(fromTime types.Time, duration types.Duration) []*Job {
	maxFinishedIdx := -1
	currTime := fromTime + types.Time(duration)
	tempTime := fromTime
	for idx, j := range q.jobs {
		if j.IsFinished() {
			panic(fmt.Sprintf("GPUJobQueue %+v passDuration %+v, j.IsFinished() is true, j = %+v", q, duration, j))
		}
		j.executesFor(q.gpu, tempTime, types.Duration(currTime-tempTime))
		if !j.IsFinished() {
			break
		}
		maxFinishedIdx = int(math.Max(float64(maxFinishedIdx), float64(idx)))
		tempTime = j.FinishExecutionTime()
	}
	finished := q.jobs[:maxFinishedIdx+1]
	q.jobs = q.jobs[maxFinishedIdx+1:]
	return finished
}

func (q *GPUJobQueue) FirstJobRemainingDuration() types.Duration {
	if len(q.jobs) == 0 {
		return types.Duration(math.Inf(1))
	}
	return q.jobs[0].RemainingDuration(q.gpu.Type())
}
