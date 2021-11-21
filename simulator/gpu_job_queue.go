package simulator

import (
	"fmt"
	"math"
)

type GPUJobQueue struct {
	gpu  *GPU
	jobs []*Job
}

func (q *GPUJobQueue) GPU() *GPU {
	return q.gpu
}

func (q *GPUJobQueue) Jobs() []*Job {
	return q.jobs
}

func (q *GPUJobQueue) SetJobs(jobs []*Job) {
	q.jobs = jobs
	// those jobs not on the first rank cannot have 'running' status
	for i := 1; i < len(q.jobs); i++ {
		q.jobs[i].setNotRunning()
	}
}

func NewGPUJobQueue(gpu *GPU) *GPUJobQueue {
	return &GPUJobQueue{
		gpu:  gpu,
		jobs: make([]*Job, 0),
	}
}

func (q *GPUJobQueue) PassDuration(fromTime Time, duration Duration) []*Job {
	maxFinishedIdx := -1
	currTime := fromTime + Time(duration)
	tempTime := fromTime
	for idx, j := range q.jobs {
		if j.IsFinished() {
			panic(fmt.Sprintf("GPUJobQueue %+v passDuration %+v, j.IsFinished() is true, j = %+v", q, duration, j))
		}
		j.ExecutesFor(q.gpu, tempTime, Duration(currTime-tempTime))
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

func (q *GPUJobQueue) FirstJobRemainingDuration() Duration {
	if len(q.jobs) == 0 {
		return Duration(math.Inf(1))
	}
	return q.jobs[0].RemainingDuration(q.gpu)
}

func (q *GPUJobQueue) Clone() *GPUJobQueue {
	clonedJobs := make([]*Job, 0, len(q.jobs))
	for _, j := range q.jobs {
		clonedJobs = append(clonedJobs, j.Clone())
	}
	cloned := &GPUJobQueue{
		gpu:  q.gpu,
		jobs: clonedJobs,
	}
	return cloned
}
