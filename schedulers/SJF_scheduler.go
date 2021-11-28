package schedulers

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/simulator"
	"fmt"
	"math"
	"sort"
)

// SJFScheduler
// 采取了Shortest Job First策略。分别实现了抢占与非抢占。
// 采取调度器内部做任务缓存队列的思想。将所有未在GPU上运行的任务缓存到sortedWaitingJobs中来。
// 这就是说集群中的每个GPUJobQueue最多只有一个任务在队列中。
// 每当有新的任务到来时，将它按照remainingDuration排序的顺序，插入到sortedWaitingJobs中。
// 当发现有空闲的GPU槽，并且存在任务正在等待队列中时，将进行调度。
// 一旦当收到有job在GPU上运行结束的消息，能够确定的是那个GPU一定能够空闲下来。
// SJF具体的算法过程：sortedWaitingJobs是按照GPUType分类的，在迭代中选择下一个要上GPU的任务时，
// 将所有GPUType中最短的任务，与可能的全部GPUQueue做一一对应，在这些两两组合中，选择一个最短的任务。
// 将它放置到GPU队列后，再迭代这个过程。
// 抢占与非抢占的区别就在于，非抢占只遍历那些空闲的GPU，而抢占式，则会先将GPU队列中的全部任务卸下，
// 让它们全部变为空闲的GPU，再按照非抢占的调度算法执行即可。
type SJFScheduler struct {
	// 可指定是否为抢占式的调度。
	// 如果是可抢占式的，则能够将正在运行的任务卸下，完全地重新排序。
	// 如果是非抢占式的，则集群队列中的正在运行的任务无法被调度开。
	// 抢占式的SJF（SRTF）是一个理想化的调度，因为在实际中，如果每次有新的任务到来都会造成当前运行的任务被调度开，
	// 则会造成超大的overhead。这只存在于理论当中。
	preemptive bool

	cluster *simulator.Cluster

	// 等待队列中的所有任务，其分别在每种类型的GPU上，按照RemainingDuration排序。
	sortedWaitingJobs map[simulator.GPUType][]*simulator.Job
}

func NewSJFScheduler(preemptive bool) *SJFScheduler {
	return &SJFScheduler{
		preemptive: preemptive,
	}
}

func (s *SJFScheduler) DoSchedule() {
	if s.preemptive {
		s.doSchedulePreemptive()
	} else {
		s.doScheduleNonPreemptive()
	}
}

func (s *SJFScheduler) doScheduleNonPreemptive() {
	for s.hasWaitingJob() && s.hasEmptyGPUQueue() {
		// 从waitingJobs中，在全部可能的EmptyGPUSlot上，挑选一个速度最快的。
		emptyQueues := s.getEmptyGPUQueues()
		var targetJob *simulator.Job = nil
		var targetQueue *simulator.GPUJobQueue = nil
		leastRemainingDuration := simulator.Duration(math.Inf(1))
		// 遍历全部的waiting job，按照gpu type进行分类，在每个waitingJobs上找首个job（即在这个类型上剩余执行时间最短的任务）
		// 遍历结束后，找到一个速度最快的任务。
		for gpuType, waitingJobs := range s.sortedWaitingJobs {
			if len(waitingJobs) == 0 {
				continue
			}
			firstWaitingJob := waitingJobs[0]
			var candidateQueue *simulator.GPUJobQueue = nil
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
			if targetJob == nil {
				targetJob, targetQueue = firstWaitingJob, candidateQueue
				leastRemainingDuration = targetJob.RemainingDuration(gpuType)
			} else if rd := firstWaitingJob.RemainingDuration(gpuType); rd < leastRemainingDuration {
				targetJob, targetQueue = firstWaitingJob, candidateQueue
				leastRemainingDuration = rd
			}
		}
		if targetJob == nil || targetQueue == nil {
			panic("SJFScheduler targetJob == nil || targetQueue == nil")
		}
		s.removeFromSortedWaitingJobs(targetJob)
		targetQueue.SetJobs([]*simulator.Job{targetJob})
	}
}

func (s *SJFScheduler) doSchedulePreemptive() {
	// 如果做抢占式的调度，就将所有正在运行的任务都撤下来，放入到sortedWaitingJobs中。
	for _, queue := range s.cluster.GPUJobQueues() {
		jobs := queue.ClearQueue()
		for _, job := range jobs {
			s.insertJob2SortedWaitingJobs(job)
		}
	}
	// 之后再按照非抢占式的调度来做就ok了。
	s.doScheduleNonPreemptive()
}

func (s *SJFScheduler) hasWaitingJob() bool {
	for _, l := range s.sortedWaitingJobs {
		if len(l) > 0 {
			return true
		}
	}
	return false
}

func (s *SJFScheduler) insertJob2SortedWaitingJobs(job *simulator.Job) {
	for _, gpuType := range s.cluster.GPUTypes() {
		ls := s.sortedWaitingJobs[gpuType]
		target := job.RemainingDuration(gpuType)
		i := sort.Search(len(ls), func(i int) bool {
			return ls[i].RemainingDuration(gpuType) >= target
		})
		s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, i, ls)
	}
}

func (s *SJFScheduler) removeFromSortedWaitingJobs(job *simulator.Job) {
	for _, gpuType := range s.cluster.GPUTypes() {
		ls := s.sortedWaitingJobs[gpuType]
		target := job.RemainingDuration(gpuType)
		i := sort.Search(len(ls), func(i int) bool {
			return ls[i].RemainingDuration(gpuType) >= target
		})
		if ls[i].RemainingDuration(gpuType) != target {
			panic("SJFScheduler removeFromSortedWaitingJobs ls[i].RemainingDuration(gpuType) != target")
		}
		var targetIdx = -1
		for ls[i].RemainingDuration(gpuType) == target {
			if ls[i].JobName() == job.JobName() {
				targetIdx = i
				break
			}
			i++
		}
		if targetIdx == -1 {
			panic("SJFScheduler removeFromSortedWaitingJobs targetIdx == -1")
		}
		var removed *simulator.Job
		removed, s.sortedWaitingJobs[gpuType] = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(targetIdx, ls)
		if removed != job {
			panic("SJFScheduler removeFromSortedWaitingJobs removed != job")
		}
	}
}

func (s *SJFScheduler) hasEmptyGPUQueue() bool {
	for _, queue := range s.cluster.GPUJobQueues() {
		if len(queue.Jobs()) == 0 {
			return true
		}
	}
	return false
}

func (s *SJFScheduler) getEmptyGPUQueues() []*simulator.GPUJobQueue {
	queues := make([]*simulator.GPUJobQueue, 0, len(s.cluster.GPUJobQueues()))
	for _, queue := range s.cluster.GPUJobQueues() {
		if len(queue.Jobs()) == 0 {
			queues = append(queues, queue)
		}
	}
	return queues
}

func (s *SJFScheduler) SetCluster(cluster *simulator.Cluster) {
	s.cluster = cluster
	s.sortedWaitingJobs = make(map[simulator.GPUType][]*simulator.Job)
	for _, gpuType := range s.cluster.GPUTypes() {
		s.sortedWaitingJobs[gpuType] = make([]*simulator.Job, 0)
	}
}

func (s *SJFScheduler) OnScheduleEvent(event simulator.ScheduleEvent) {
	switch e := event.(type) {
	case *simulator.ScheduleEventJobsArrived:
		{
			for _, jobMeta := range e.JobMetas() {
				s.insertJob2SortedWaitingJobs(simulator.NewJob(jobMeta.JobName()))
			}
			s.DoSchedule()
		}
	case *simulator.ScheduleEventJobsFinished:
		{
			if !s.hasEmptyGPUQueue() {
				panic("!s.hasEmptyGPUQueue() when some jobs finished.")
			}
			s.DoSchedule()
		}
	}
}

func (s *SJFScheduler) NextActiveScheduleTime() simulator.Time {
	return simulator.Time(math.Inf(1))
}

func (s *SJFScheduler) Name() string {
	return fmt.Sprintf("SJFScheduler[preemptive=%v]", s.preemptive)
}
