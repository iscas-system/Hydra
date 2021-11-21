package schedulers

import (
	"DES-go/simulator"
	"math"
)

type DummyScheduler struct {
	cluster             *simulator.Cluster
	nextScheduleToGPUID int
	lastScheduleTime    simulator.Time
	maxGPUJobQueueID    int
	unscheduledJobMetas []*simulator.JobMeta

	unscheduledJobsCacheLength int
	maxScheduleInterval        int
}

func NewDummyScheduler() *DummyScheduler {
	// some casual dummy configs
	unscheduledJobsCacheLength := 100
	maxScheduleInterval := 1000
	return &DummyScheduler{
		unscheduledJobMetas:        make([]*simulator.JobMeta, 0, unscheduledJobsCacheLength),
		maxScheduleInterval:        maxScheduleInterval,
		unscheduledJobsCacheLength: unscheduledJobsCacheLength,
	}
}

func (d *DummyScheduler) DoSchedule() {
	if d.unscheduledJobMetas == nil {
		panic("DummyScheduler d.unscheduledJobMetas == nil")
	}
	jobs := make([]*simulator.Job, 0, len(d.unscheduledJobMetas))
	for _, job := range d.unscheduledJobMetas {
		jobs = append(jobs, simulator.NewJob(job.JobName()))
	}
	targetJobQueue := d.cluster.GPUJobQueues()[simulator.GPUID(d.nextScheduleToGPUID)]
	targetJobQueue.SetJobs(append(targetJobQueue.Jobs(), jobs...))

	d.nextScheduleToGPUID = (d.nextScheduleToGPUID + 1) % d.maxGPUJobQueueID
	d.lastScheduleTime = d.cluster.Now()
	d.unscheduledJobMetas = d.unscheduledJobMetas[:0]
}

func (d *DummyScheduler) SetCluster(cluster *simulator.Cluster) {
	d.cluster = cluster
	d.nextScheduleToGPUID = 0
	d.lastScheduleTime = d.cluster.Now()

	d.maxGPUJobQueueID = math.MinInt
	for _, gpuList := range d.cluster.GPUs() {
		for _, gpu := range gpuList {
			d.maxGPUJobQueueID = int(math.Max(float64(gpu.ID()) + 1, float64(d.maxGPUJobQueueID)))
		}
	}
}

func (d *DummyScheduler) OnScheduleEvent(event simulator.ScheduleEvent) {
	switch e := event.(type) {
	case *simulator.ScheduleEventDurationPassed:
		{
			if d.cluster.Now()-d.lastScheduleTime >= simulator.Time(d.maxScheduleInterval) {
				d.DoSchedule()
			}
		}
	case *simulator.ScheduleEventJobsArrived:
		{
			d.unscheduledJobMetas = append(d.unscheduledJobMetas, e.JobMetas()...)
			if len(d.unscheduledJobMetas) > d.unscheduledJobsCacheLength {
				d.DoSchedule()
			}
		}
	}
}

func (d *DummyScheduler) NextActiveScheduleTime() simulator.Time {
	if len(d.unscheduledJobMetas) > 0 {
		return simulator.Time(d.maxScheduleInterval) - (d.cluster.Now() - d.lastScheduleTime)
	}
	return simulator.Time(math.Inf(1))
}

func (d *DummyScheduler) Name() string {
	return "DummyScheduler"
}