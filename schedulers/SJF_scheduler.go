package schedulers

import (
	"DES-go/simulator"
	"container/list"
	"fmt"
	"math"
)


type SJFScheduler struct {
	// 可指定是否为抢占式的调度。
	// 如果是可抢占式的，则能够将正在运行的任务卸下，完全地重新排序。
	// 如果是非抢占式的，则集群队列中的正在运行的任务无法被调度开。
	preemptive bool

	cluster *simulator.Cluster
	// gpu2AliveJobMetasSortedByLength 存储了集群中的当前存活的，已经调度到每个GPU上的任务元信息。
	// 相当于一份集群GPU任务队列的副本。每当有任务执行结束时更新这个数据结构，维护一致性。
	// 之所以使用这个数据结构，原因在于Shortest Job First涉及到插入操作，使用Slice相当难受。
	// 每当有新任务到来时，经过SJF过程，将新任务都先插入到该副本中，随后再通过遍历，将新的任务部署到集群中。
	gpu2AliveJobMetasSortedByLength map[simulator.GPUID]*list.List

	newlyArrivedJobMetas []*simulator.JobMeta
}

func NewSJFScheduler(preemptive bool) *SJFScheduler {
	return &SJFScheduler{
		preemptive: preemptive,
	}
}

func (s *SJFScheduler) DoSchedule() {
	if s.newlyArrivedJobMetas == nil {
		panic("SJFScheduler s.newlyArrivedJobMetas is nil")
	}
	newlyArrivedJobMetas := s.newlyArrivedJobMetas
	s.newlyArrivedJobMetas = nil

	// 以下结构体定义了一个尝试使用SJF顺序插入到某个GPU队列上的结果。
	// jobStartTimeAfterInsert表示按照如果按照SJF顺序将任务插入到该队列后，该任务的开始时间。
	// doActualInsertionFunc是一个闭包，它能够真正地完成将该任务插入到队列中的动作。
	type attemptInsertionInfo struct {
		onWhichGPU *simulator.GPU
		jobStartTimeAfterInsert simulator.Time
		doActualInsertionFunc   func()
	}
	// 以下定义一个工具函数，返回结果为：尝试将一个新的任务，按照SJF顺序插入到某GPU队列后的信息。
	getAttemptInsertionInfo := func(jobMeta *simulator.JobMeta, gpu *simulator.GPU, jobMetaList *list.List) *attemptInsertionInfo {
		e := jobMetaList.Front()
		startTime := simulator.Time(0.)
		if !s.preemptive {
			// 如果是非抢占式调度，则第一个任务如果是在运行状态，则不能将新的任务调度到它的前面。
			if e != nil {
				meta := e.Value.(*simulator.JobMeta)
				// 由于有可能在调度过程中，将一个新的任务插入到s.gpu2AliveJobMetasSortedByLength中，所以需要检查与集群队列中的第一个是否相同。
				jobQueue := s.cluster.GPUJobQueues()[gpu.ID()].Jobs()
				if len(jobQueue) > 0 {
					firstJobInQueue := jobQueue[0]
					if firstJobInQueue.JobName() == meta.JobName() && firstJobInQueue.IsRunning() {
						// 如果第一个任务是在运行状态，则将它的剩余运行时间加到startTime上来。
						startTime += simulator.Time(firstJobInQueue.RemainingDuration(gpu))
						// 跳过这个正在运行的任务。
						e = e.Next()
					}
				}
			}
		}
		for e != nil {
			next := e.Next()
			currJobMeta := e.Value.(*simulator.JobMeta)
			if currJobMeta.Durations()[gpu.Type()] > jobMeta.Durations()[gpu.Type()] {
				return &attemptInsertionInfo{
					onWhichGPU: gpu,
					jobStartTimeAfterInsert:  startTime,
					doActualInsertionFunc:      func() {
						jobMetaList.InsertBefore(jobMeta, e)
					},
				}
			}
			startTime += simulator.Time(currJobMeta.Durations()[gpu.Type()])
			e = next
		}
		// 如果队列为空，或者为该队列运行时间最长的任务时，直接插入到队列尾部。
		return &attemptInsertionInfo{
			onWhichGPU: gpu,
			jobStartTimeAfterInsert:  startTime,
			doActualInsertionFunc:      func() {
				jobMetaList.PushBack(jobMeta)
			},
		}
	}

	// 接下来将每个新来到的任务，插入到s.gpu2AliveJobMetasSortedByLength中。
	// 插入的位置通过SJF得到，随后比较这个任务按照SJF顺序插入到每个队列上的任务开始时间，挑选那个最早能够开始执行的队列进行插入。
	// 如果有一些开始时间相同的队列，则选取那个执行时间最短的任务。（待确认，是否需要计算后续任务的JCT增长大小？异构的SFJ没有想象中的trivial，需要再确认）
	for _, jobMeta := range newlyArrivedJobMetas {
		minStartTimeInfo := &attemptInsertionInfo{
			jobStartTimeAfterInsert: simulator.Time(math.Inf(1)),
		}
		for gpuID, jobMetaList := range s.gpu2AliveJobMetasSortedByLength {
			gpu := s.cluster.GPU(gpuID)
			currInfo := getAttemptInsertionInfo(jobMeta, gpu, jobMetaList)
			if currInfo.jobStartTimeAfterInsert < minStartTimeInfo.jobStartTimeAfterInsert {
				minStartTimeInfo = currInfo
			}
			if currInfo.jobStartTimeAfterInsert == minStartTimeInfo.jobStartTimeAfterInsert {
				// 如果开始时间相同，就选择那个执行时间最短的。
				if jobMeta.Duration(gpu) < jobMeta.Duration(minStartTimeInfo.onWhichGPU) {
					minStartTimeInfo = currInfo
				}
			}
		}
		minStartTimeInfo.doActualInsertionFunc()
	}


	// 定义工具函数，用于插入一个新的任务到某个已有任务队列中。
	insertNewJob := func(jobMeta *simulator.JobMeta, idx int, jobs []*simulator.Job) []*simulator.Job {
		back := append([]*simulator.Job{}, jobs[idx:]...)
		res := append(jobs[:idx], simulator.NewJob(jobMeta.JobName()))
		res = append(res, back...)
		return res
	}

	// s.gpu2AliveJobMetasSortedByLength中已经有了新的任务信息，分别在每个GPU队列上，
	// 联合地按照顺序遍历每个任务，找到那些在集群中还没有的任务，即就是要插入的新的任务。
	for gpuID, queue := range s.cluster.GPUJobQueues() {
		jobsInQueue := queue.Jobs()
		jobMetaList := s.gpu2AliveJobMetasSortedByLength[gpuID]
		idx := 0
		metaElem := jobMetaList.Front()
		for metaElem != nil && idx < len(jobsInQueue) {
			meta := metaElem.Value.(*simulator.JobMeta)
			if meta.JobName() != jobsInQueue[idx].JobName() {
				// jobMeta的JobName与在集群中当前任务的JobName不相同，则需要将新的jobMeta作为新任务插入到当前位置。
				jobsInQueue = insertNewJob(meta, idx, jobsInQueue)
			}
			metaElem = metaElem.Next()
			idx++
		}
		if idx != len(jobsInQueue) {
			panic("SJFScheduler idx != len(jobsInQueue)")
		}
		// 如果jobMetaList还有剩余，则说明这部分任务都要插入到当前任务队列的最末尾。
		for metaElem != nil {
			meta := metaElem.Value.(*simulator.JobMeta)
			jobsInQueue = insertNewJob(meta, len(jobsInQueue), jobsInQueue)
			metaElem = metaElem.Next()
		}
		// 最后，将最新的jobs队列设置到集群队列中。
		queue.SetJobs(jobsInQueue)
	}

	// 正确性检查，用于debug
	for gpuID, queue := range s.cluster.GPUJobQueues() {
		jobsInQueue := queue.Jobs()
		jobMetaList := s.gpu2AliveJobMetasSortedByLength[gpuID]
		idx := 0
		metaElem := jobMetaList.Front()
		for metaElem != nil && idx < len(jobsInQueue) {
			meta := metaElem.Value.(*simulator.JobMeta)
			if meta.JobName() != jobsInQueue[idx].JobName() {
				panic("SJFScheduler Correctness check failed, meta.JobName() != jobsInQueue[idx].JobName()")
			}
			metaElem = metaElem.Next()
			idx++
		}
		if idx != len(jobsInQueue) {
			panic("SJFScheduler Correctness check failed, idx != len(jobsInQueue)")
		}
	}

}

func (s *SJFScheduler) SetCluster(cluster *simulator.Cluster) {
	s.cluster = cluster
	s.gpu2AliveJobMetasSortedByLength = make(map[simulator.GPUID]*list.List)
	for gpuID := range cluster.GPUJobQueues() {
		s.gpu2AliveJobMetasSortedByLength[gpuID] = list.New()
	}
}

func (s *SJFScheduler) OnScheduleEvent(event simulator.ScheduleEvent) {
	switch e := event.(type) {
	case *simulator.ScheduleEventJobsArrived:
		{
			s.newlyArrivedJobMetas = e.JobMetas()
			s.DoSchedule()
		}
	case *simulator.ScheduleEventJobsFinished:
		{
			s.onJobsExit(e.Jobs())
		}
	}
}

func (s *SJFScheduler) onJobsExit(jobs []*simulator.Job) {
	finishedJobNames := make(map[simulator.JobName]bool)
	for _, job := range jobs {
		finishedJobNames[job.JobName()] = true
	}
	for _, jobMetas := range s.gpu2AliveJobMetasSortedByLength {
		e := jobMetas.Front()
		for e != nil {
			next := e.Next()
			curr := e.Value.(*simulator.JobMeta)
			if _, ok := finishedJobNames[curr.JobName()]; ok {
				jobMetas.Remove(e)
			}
			e = next
		}
	}
}

func (s *SJFScheduler) NextActiveScheduleTime() simulator.Time {
	return simulator.Time(math.Inf(1))
}

func (s *SJFScheduler) Name() string {
	return fmt.Sprintf("SJFScheduler[preemptive=%v]", s.preemptive)
}
