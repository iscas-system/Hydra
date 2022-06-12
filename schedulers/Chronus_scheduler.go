package schedulers

import (
	"DES-go/schedulers/types"
	"fmt"
	"math"
	"sort"
)

// ChronusScheduler
type ChronusScheduler struct {
	*SchedulerTemplate
}

func NewChronusScheduler() *ChronusScheduler {
	template := NewGreedySchedulerTemplate()
	edf := &ChronusScheduler{
		template,
	}
	template.impl = edf
	return edf
}

func (s *ChronusScheduler) pickTarget(emptyQueues []types.GPUJobQueue) (types.Job, types.GPUJobQueue) {
	var targetJob types.Job = nil
	//var targetQueue types.GPUJobQueue = nil
	//leastRemainingDuration := types.Duration(math.Inf(1))
	jobsMap := make(map[types.JobName]types.Job)
	for _, jobs := range s.sortedWaitingJobs {
		for _, job := range jobs {
			if _, ok := jobsMap[job.JobName()]; !ok {
				jobsMap[job.JobName()] = job
			}
		}
	}
	waitingJobs := make([]types.Job, 0, len(jobsMap))
	for _, job := range jobsMap {
		waitingJobs = append(waitingJobs, job)
	}
	sort.Slice(waitingJobs, func(i, j int) bool {
		if waitingJobs[i].HasDDL() && waitingJobs[j].HasDDL() {
			return waitingJobs[i].JobMeta().DDL() < waitingJobs[j].JobMeta().DDL()
		} else if waitingJobs[i].HasDDL() && !waitingJobs[j].HasDDL() {
			return true
		} else if !waitingJobs[i].HasDDL() && waitingJobs[j].HasDDL() {
			return false
		} else {
			return waitingJobs[i].RemainingDuration("A100") < waitingJobs[j].RemainingDuration("A100")
		}
	})
	minDis := types.Time(math.Inf(1))
	waitingJob := waitingJobs[0]
	targetGPU := types.GPU(nil)
	//for _, waitingJob := range waitingJobs {
		for _, queue := range emptyQueues {
			rd := types.Time(waitingJob.RemainingDuration(queue.GPU().Type()))
			if waitingJob.HasDDL() {
				dis := rd - waitingJob.JobMeta().DDL()
				if dis < minDis {
					minDis = dis
					targetJob = waitingJob
					targetGPU = queue.GPU()
				}
			} else {
				if minDis == types.Time(math.Inf(1)) {
					if targetJob == nil {
						targetJob = waitingJob
						targetGPU = queue.GPU()
					} else {
						rdt := types.Time(targetJob.RemainingDuration(queue.GPU().Type()))
						rd := types.Time(waitingJob.RemainingDuration(queue.GPU().Type()))
						if rd < rdt {
							targetJob = waitingJob
							targetGPU = queue.GPU()
						}
					}
				}
			}
		}
	//}
	return waitingJob, s.cluster.GPUJobQueues()[targetGPU.ID()]
	//for gpuType, waitingJobs := range s.sortedWaitingJobs {
	//	if len(waitingJobs) == 0 {
	//		continue
	//	}
	//	firstWaitingJob := waitingJobs[0]
	//	var candidateQueue types.GPUJobQueue = nil
	//	for _, queue := range emptyQueues {
	//		if queue.GPU().Type() != gpuType {
	//			continue
	//		}
	//		if candidateQueue == nil {
	//			candidateQueue = queue
	//			break
	//		}
	//	}
	//	if candidateQueue == nil {
	//		continue
	//	}
	//	updateTarget := func(firstWaitingJob types.Job, candidateQueue types.GPUJobQueue) {
	//		targetJob, targetQueue = firstWaitingJob, candidateQueue
	//		leastRemainingDuration = targetJob.RemainingDuration(gpuType)
	//	}
	//	if targetJob == nil {
	//		updateTarget(firstWaitingJob, candidateQueue)
	//		continue
	//	}
	//	if targetJob.HasDDL() {
	//		if !firstWaitingJob.HasDDL() {
	//			continue
	//		}
	//		if firstWaitingJob.HasDDL() && firstWaitingJob.JobMeta().DDL() < targetJob.JobMeta().DDL() {
	//			updateTarget(firstWaitingJob, candidateQueue)
	//			continue
	//		}
	//	}
	//	if !targetJob.HasDDL() {
	//		if firstWaitingJob.HasDDL() {
	//			updateTarget(firstWaitingJob, candidateQueue)
	//			continue
	//		}
	//		if !firstWaitingJob.HasDDL() {
	//			if firstWaitingJob.RemainingDuration(gpuType) < leastRemainingDuration {
	//				updateTarget(firstWaitingJob, candidateQueue)
	//				continue
	//			}
	//		}
	//	}
	//}
	//return targetJob, targetQueue
}

func (s *ChronusScheduler) insertJob2SortedWaitingJobs(job types.Job) {
//nextGPU:
	for _, gpuType := range s.cluster.GPUTypes() {
		waitingJobs := s.sortedWaitingJobs[gpuType]
		waitingJobs = append(waitingJobs, job)
		sort.Slice(waitingJobs, func(i, j int) bool {
			if waitingJobs[i].HasDDL() && waitingJobs[j].HasDDL() {
				return waitingJobs[i].JobMeta().DDL() < waitingJobs[j].JobMeta().DDL()
			} else if waitingJobs[i].HasDDL() && !waitingJobs[j].HasDDL() {
				return true
			} else if !waitingJobs[i].HasDDL() && waitingJobs[j].HasDDL() {
				return false
			} else {
				return waitingJobs[i].RemainingDuration("A100") < waitingJobs[j].RemainingDuration("A100")
			}
		})
		s.sortedWaitingJobs[gpuType] = waitingJobs
		//if job.HasDDL() {
		//	for idx, jobInWaitingList := range ls {
		//		if jobInWaitingList.HasDDL() && jobInWaitingList.JobMeta().DDL() < job.JobMeta().DDL() {
		//			continue
		//		}
		//		s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, idx, ls)
		//		continue nextGPU
		//	}
		//} else {
		//	for idx, jobInWaitingList := range ls {
		//		if jobInWaitingList.HasDDL() {
		//			continue
		//		}
		//		if jobInWaitingList.RemainingDuration(gpuType) < job.RemainingDuration(gpuType) {
		//			continue
		//		}
		//		s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, idx, ls)
		//		continue nextGPU
		//	}
		//}
		//s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, len(ls), ls)
	}
}

func (s *ChronusScheduler) Name() string {
	return fmt.Sprintf("ChronusScheduler")
}

func (s *ChronusScheduler) Info() interface{} {
	return s.Name()
}
