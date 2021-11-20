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
	jobQueue := d.cluster.GpuJobQueues()[simulator.GPUID(d.nextScheduleToGPUID)]
	jobQueue.SetJobs(append(jobQueue.Jobs(), jobs...))
	d.nextScheduleToGPUID = (d.nextScheduleToGPUID + 1) % d.maxGPUJobQueueID
	d.lastScheduleTime = d.cluster.Now()
}

func (d *DummyScheduler) SetCluster(cluster *simulator.Cluster) {
	d.cluster = cluster
	d.nextScheduleToGPUID = 0
	d.lastScheduleTime = d.cluster.Now()

	d.maxGPUJobQueueID = math.MinInt
	for _, gpuList := range d.cluster.GPUs() {
		for _, gpu := range gpuList {
			d.maxGPUJobQueueID = int(math.Max(float64(gpu.ID()), float64(d.maxGPUJobQueueID)))
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
	return simulator.Time(d.maxScheduleInterval) - (d.cluster.Now() - d.lastScheduleTime)
}
