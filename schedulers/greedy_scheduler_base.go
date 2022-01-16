package schedulers

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/schedulers/types"
	"DES-go/simulator"
	"fmt"
	"math"
	"time"
)

// GreedySchedulerTemplate 类似于EDF或SJF之类的算法，它们的贪心规则类似，可以抽象出模板方法
type GreedySchedulerTemplate struct {
	cluster types.Cluster

	// 等待队列中的所有任务，其分别在每种类型的GPU上，按照RemainingDuration排序。
	sortedWaitingJobs map[types.GPUType][]types.Job

	DoScheduleCalls []*types.DoScheduleCallRecord
	impl            GreedyScheduler
}

type GreedyScheduler interface {
	types.Scheduler
	insertJob2SortedWaitingJobs(job types.Job)
	pickTarget(emptyQueues []types.GPUJobQueue) (types.Job, types.GPUJobQueue)
}

func NewGreedySchedulerTemplate() *GreedySchedulerTemplate {
	return &GreedySchedulerTemplate{
		DoScheduleCalls: make([]*types.DoScheduleCallRecord, 0),
	}
}

func (s *GreedySchedulerTemplate) DoSchedule() {
	start := time.Now()
	s.doSchedule()
	duration := time.Since(start)
	s.DoScheduleCalls = append(s.DoScheduleCalls, &types.DoScheduleCallRecord{Duration: duration})
}

func (s *GreedySchedulerTemplate) doSchedule() {
	for s.hasWaitingJob() && s.hasEmptyGPUQueue() {
		// 从waitingJobs中，在全部可能的EmptyGPUSlot上，挑选一个速度最快的。
		emptyQueues := s.getEmptyGPUQueues()
		targetJob, targetQueue := s.impl.pickTarget(emptyQueues)
		// 遍历全部的waiting job，按照gpu type进行分类，在每个waitingJobs上找首个job（即在这个类型上剩余执行时间最短的任务）
		// 遍历结束后，找到一个速度最快的任务。
		if targetJob == nil || targetQueue == nil {
			panic("GreedySchedulerTemplate targetJob == nil || targetQueue == nil")
		}
		s.removeFromSortedWaitingJobs(targetJob)
		targetQueue.SetJobs(targetJob)
	}
}

func (s *GreedySchedulerTemplate) pickTarget(emptyQueues []types.GPUJobQueue) (types.Job, types.GPUJobQueue) {
	panic("GreedySchedulerTemplate pickTarget cannot be called.")
}

func (s *GreedySchedulerTemplate) hasWaitingJob() bool {
	for _, l := range s.sortedWaitingJobs {
		if len(l) > 0 {
			return true
		}
	}
	return false
}

func (s *GreedySchedulerTemplate) insertJob2SortedWaitingJobs(job types.Job) {
	panic("GreedySchedulerTemplate insertJob2SortedWaitingJobs Cannot be called.")
}

func (s *GreedySchedulerTemplate) removeFromSortedWaitingJobs(job types.Job) {
	for _, gpuType := range s.cluster.GPUTypes() {
		ls := s.sortedWaitingJobs[gpuType]
		targetIdx := -1
		for idx, jobInWaitingList := range ls {
			if jobInWaitingList.JobName() == job.JobName() {
				targetIdx = idx
			}
		}
		if targetIdx == -1 {
			panic("GreedySchedulerTemplate removeFromSortedWaitingJobs targetIdx == -1")
		}
		var removed types.Job
		removed, s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(targetIdx, ls)
		if removed != job {
			panic("GreedySchedulerTemplate removeFromSortedWaitingJobs removed != job")
		}
	}
}

func (s *GreedySchedulerTemplate) hasEmptyGPUQueue() bool {
	for _, queue := range s.cluster.GPUJobQueues() {
		if len(queue.Jobs()) == 0 {
			return true
		}
	}
	return false
}

func (s *GreedySchedulerTemplate) getEmptyGPUQueues() []types.GPUJobQueue {
	queues := make([]types.GPUJobQueue, 0, len(s.cluster.GPUJobQueues()))
	for _, queue := range s.cluster.GPUJobQueues() {
		if len(queue.Jobs()) == 0 {
			queues = append(queues, queue)
		}
	}
	return queues
}

func (s *GreedySchedulerTemplate) SetCluster(cluster types.Cluster) {
	s.cluster = cluster
	s.sortedWaitingJobs = make(map[types.GPUType][]types.Job)
	for _, gpuType := range s.cluster.GPUTypes() {
		s.sortedWaitingJobs[gpuType] = make([]types.Job, 0)
	}
}

func (s *GreedySchedulerTemplate) OnScheduleEvent(event types.ScheduleEvent) {
	switch e := event.(type) {
	case *types.ScheduleEventJobsArrived:
		{
			for _, jobMeta := range e.JobMetas() {
				s.impl.insertJob2SortedWaitingJobs(simulator.NewJob(jobMeta.JobName()))
			}
			s.DoSchedule()
		}
	case *types.ScheduleEventJobsFinished:
		{
			if !s.hasEmptyGPUQueue() {
				panic("!s.hasEmptyGPUQueue() when some jobs finished.")
			}
			s.DoSchedule()
		}
	}
}

func (s *GreedySchedulerTemplate) NextActiveScheduleTime() types.Time {
	return types.Time(math.Inf(1))
}

func (s *GreedySchedulerTemplate) Name() string {
	return fmt.Sprintf("GreedySchedulerTemplate")
}

func (s *GreedySchedulerTemplate) Info() interface{} {
	return s.Name()
}

func (s *GreedySchedulerTemplate) Record() *types.SchedulerRecord {
	return &types.SchedulerRecord{
		DoScheduleRecords: s.DoScheduleCalls,
	}
}
