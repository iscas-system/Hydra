package simulator

import "math"

type Cluster struct {
	gpuType2CountConfig map[GPUType]int
	gpus                map[GPUType][]*GPU
	timer               Time
	gpuJobQueues        map[GPUID]*GPUJobQueue
}

func (c *Cluster) GpuJobQueues() map[GPUID]*GPUJobQueue {
	return c.gpuJobQueues
}

func NewCluster(gpuType2CountConfig map[GPUType]int) *Cluster {
	gpus := make(map[GPUType][]*GPU)
	gpuID := GPUID(0)
	for gpuType, count := range gpuType2CountConfig {
		l := make([]*GPU, 0, count)
		for i := 0; i < count; i++ {
			l = append(l, NewGPU(gpuID, gpuType))
			gpuID++
		}
		gpus[gpuType] = l
	}

	gpuJobQueues := make(map[GPUID]*GPUJobQueue)
	for _, gpuList := range gpus {
		for _, gpu := range gpuList {
			gpuJobQueues[gpu.ID()] = NewGPUJobQueue(gpu)
		}
	}

	return &Cluster{
		gpuType2CountConfig: gpuType2CountConfig,
		gpus:                gpus,
		timer:               Time(-1),
		gpuJobQueues:        gpuJobQueues,
	}
}

func (c *Cluster) GPUs() map[GPUType][]*GPU {
	return c.gpus
}

func (c *Cluster) StartServe() {
	c.timer = 0
}

func (c *Cluster) IsServing() bool {
	return c.timer != -1
}

func (c *Cluster) Now() Time {
	return c.timer
}

func (c *Cluster) PassDuration(duration Duration) []*Job {
	if !c.IsServing() {
		panic("Cluster PassDuration called when is not serving")
	}
	fromTime := c.timer
	c.timer += Time(duration)
	newlyFinishedJobs := make([]*Job, 0)
	// TODO use wait group to accelerate
	for _, q := range c.gpuJobQueues {
		newlyFinishedJobs = append(newlyFinishedJobs, q.PassDuration(fromTime, duration)...)
	}
	return newlyFinishedJobs
}

func (c *Cluster) ClosestTimeToFinishAnyJob() Time {
	res := math.Inf(1)
	for _, q := range c.gpuJobQueues {
		math.Min(float64(q.FirstJobRemainingDuration()), res)
	}
	return Time(res)
}
