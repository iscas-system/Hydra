package schedulers

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/schedulers/types"
	"fmt"
	"math"
)

// EDFScheduler 最早ddl优先。对于没有ddl的任务，按照SJF排序。
type EDFScheduler struct {
	*GreedySchedulerTemplate
}

func NewEDFScheduler() *EDFScheduler {
	template := NewGreedySchedulerTemplate()
	edf := &EDFScheduler{
		template,
	}
	template.impl = edf
	return edf
}

func (s *EDFScheduler) pickTarget(emptyQueues []types.GPUJobQueue) (types.Job, types.GPUJobQueue) {
	var targetJob types.Job = nil
	var targetQueue types.GPUJobQueue = nil
	leastRemainingDuration := types.Duration(math.Inf(1))
	for gpuType, waitingJobs := range s.sortedWaitingJobs {
		if len(waitingJobs) == 0 {
			continue
		}
		firstWaitingJob := waitingJobs[0]
		var candidateQueue types.GPUJobQueue = nil
		for _, queue := range emptyQueues {
			if queue.GPU().Type() != gpuType {
				continue
			}
			if candidateQueue == nil {
				candidateQueue = queue
				break
			}
		}
		if candidateQueue == nil {
			continue
		}
		updateTarget := func(firstWaitingJob types.Job, candidateQueue types.GPUJobQueue) {
			targetJob, targetQueue = firstWaitingJob, candidateQueue
			leastRemainingDuration = targetJob.RemainingDuration(gpuType)
		}
		if targetJob == nil {
			updateTarget(firstWaitingJob, candidateQueue)
			continue
		}
		if targetJob.HasDDL() {
			if !firstWaitingJob.HasDDL() {
				continue
			}
			if firstWaitingJob.HasDDL() && firstWaitingJob.JobMeta().DDL() < targetJob.JobMeta().DDL() {
				updateTarget(firstWaitingJob, candidateQueue)
				continue
			}
		}
		if !targetJob.HasDDL() {
			if firstWaitingJob.HasDDL() {
				updateTarget(firstWaitingJob, candidateQueue)
				continue
			}
			if !firstWaitingJob.HasDDL() {
				if firstWaitingJob.RemainingDuration(gpuType) < leastRemainingDuration {
					updateTarget(firstWaitingJob, candidateQueue)
					continue
				}
			}
		}
	}
	return targetJob, targetQueue
}

func (s *EDFScheduler) insertJob2SortedWaitingJobs(job types.Job) {
nextGPU:
	for _, gpuType := range s.cluster.GPUTypes() {
		ls := s.sortedWaitingJobs[gpuType]
		if job.HasDDL() {
			for idx, jobInWaitingList := range ls {
				if jobInWaitingList.HasDDL() && jobInWaitingList.JobMeta().DDL() < job.JobMeta().DDL() {
					continue
				}
				s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, idx, ls)
				continue nextGPU
			}
		} else {
			for idx, jobInWaitingList := range ls {
				if jobInWaitingList.HasDDL() {
					continue
				}
				if jobInWaitingList.RemainingDuration(gpuType) < job.RemainingDuration(gpuType) {
					continue
				}
				s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, idx, ls)
				continue nextGPU
			}
		}
		s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, len(ls), ls)
	}
}

func (s *EDFScheduler) Name() string {
	return fmt.Sprintf("EDFScheduler")
}

func (s *EDFScheduler) Info() interface{} {
	return s.Name()
}
