package schedulers

import (
	"DES-go/schedulers/types"
	"math"
)

type DummyScheduler struct {
	cluster             types.Cluster
	nextScheduleToGPUID int
	lastScheduleTime    types.Time
	maxGPUJobQueueID    int
	unscheduledJobMetas []types.JobMeta

	unscheduledJobsCacheLength int
	maxScheduleInterval        int
}

func NewDummyScheduler() *DummyScheduler {
	// some casual dummy configs
	unscheduledJobsCacheLength := 100
	maxScheduleInterval := 1000
	return &DummyScheduler{
		unscheduledJobMetas:        make([]types.JobMeta, 0, unscheduledJobsCacheLength),
		maxScheduleInterval:        maxScheduleInterval,
		unscheduledJobsCacheLength: unscheduledJobsCacheLength,
	}
}

func (d *DummyScheduler) DoSchedule() {
	if d.unscheduledJobMetas == nil {
		panic("DummyScheduler d.unscheduledJobMetas == nil")
	}
	jobs := make([]types.Job, 0, len(d.unscheduledJobMetas))
	for _, job := range d.unscheduledJobMetas {
		jobs = append(jobs, d.cluster.InitJob(job))
	}
	targetJobQueue := d.cluster.GPUJobQueues()[types.GPUID(d.nextScheduleToGPUID)]
	targetJobQueue.SetJobs(append(targetJobQueue.Jobs(), jobs...)...)

	d.nextScheduleToGPUID = (d.nextScheduleToGPUID + 1) % d.maxGPUJobQueueID
	d.lastScheduleTime = d.cluster.Now()
	d.unscheduledJobMetas = d.unscheduledJobMetas[:0]
}

func (d *DummyScheduler) SetCluster(cluster types.Cluster) {
	d.cluster = cluster
	d.nextScheduleToGPUID = 0
	d.lastScheduleTime = d.cluster.Now()

	d.maxGPUJobQueueID = math.MinInt64
	for _, gpuList := range d.cluster.GPUs() {
		for _, gpu := range gpuList {
			d.maxGPUJobQueueID = int(math.Max(float64(gpu.ID())+1, float64(d.maxGPUJobQueueID)))
		}
	}
}

func (d *DummyScheduler) OnScheduleEvent(event types.ScheduleEvent) {
	switch e := event.(type) {
	case *types.ScheduleEventDurationPassed:
		{
			if d.cluster.Now()-d.lastScheduleTime >= types.Time(d.maxScheduleInterval) {
				d.DoSchedule()
			}
		}
	case *types.ScheduleEventJobsArrived:
		{
			d.unscheduledJobMetas = append(d.unscheduledJobMetas, e.JobMetas()...)
			if len(d.unscheduledJobMetas) > d.unscheduledJobsCacheLength {
				d.DoSchedule()
			}
		}
	}
}

func (d *DummyScheduler) NextActiveScheduleTime() types.Time {
	if len(d.unscheduledJobMetas) > 0 {
		return types.Time(d.maxScheduleInterval) - (d.cluster.Now() - d.lastScheduleTime)
	}
	return types.Time(math.Inf(1))
}

func (d *DummyScheduler) Name() string {
	return "DummyScheduler"
}

func (d *DummyScheduler) Info() interface{} {
	return d.Name()
}

func (d *DummyScheduler) Record() *types.SchedulerRecord {
	return &types.SchedulerRecord{
		DoScheduleRecords: []*types.DoScheduleCallRecord{},
	}
}
