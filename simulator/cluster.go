package simulator

import (
	"DES-go/util"
	"fmt"
	"math"
	"sync"
)

type Cluster struct {
	gpuType2CountConfig map[GPUType]int
	gpus         map[GPUType][]*GPU
	Timer        Time `json:"timer"`
	GPUJobQueues map[GPUID]*GPUJobQueue `json:"gpu_job_queues"`
}

func (c *Cluster) GpuJobQueues() map[GPUID]*GPUJobQueue {
	return c.GPUJobQueues
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

	gpusConfigJson := util.Pretty(gpuType2CountConfig)
	fmt.Printf("Cluster Inited, gpus config = %s.", gpusConfigJson)

	return &Cluster{
		gpuType2CountConfig: gpuType2CountConfig,
		gpus:                gpus,
		Timer:               Time(-1),
		GPUJobQueues:        gpuJobQueues,
	}
}

func (c *Cluster) GPUs() map[GPUType][]*GPU {
	return c.gpus
}

func (c *Cluster) StartServe() {
	c.Timer = 0
}

func (c *Cluster) IsServing() bool {
	return c.Timer != -1
}

func (c *Cluster) Now() Time {
	return c.Timer
}

func (c *Cluster) PassDuration(duration Duration) []*Job {
	if !c.IsServing() {
		panic("Cluster passDuration called when is not serving")
	}
	fromTime := c.Timer
	c.Timer += Time(duration)
	newlyFinishedJobs := make([]*Job, 0)
	wg := &sync.WaitGroup{}
	finishedJobsSlice := make([][]*Job, len(c.GPUJobQueues))

	idx := 0
	for _, queue := range c.GPUJobQueues {
		queue := queue
		util.GoWithWG(wg, idx, func(idx int) {
			finishedJobsSlice[idx] = queue.PassDuration(fromTime, duration)
		})
		idx++
	}
	wg.Wait()

	for _, jobs := range finishedJobsSlice {
		newlyFinishedJobs = append(newlyFinishedJobs, jobs...)
	}

	//// TODO use wait group to accelerate
	//for _, q := range c.GPUJobQueues {
	//	newlyFinishedJobs = append(newlyFinishedJobs, q.PassDuration(fromTime, duration)...)
	//}
	return newlyFinishedJobs
}

func (c *Cluster) ClosestTimeToFinishAnyJob() Time {
	res := math.Inf(1)
	for _, q := range c.GPUJobQueues {
		res = math.Min(float64(q.FirstJobRemainingDuration()), res)
	}
	return Time(res)
}

func (c *Cluster) Expose() interface{} {
	gpu2JobQueueLength := make(map[*GPU]int)
	for _, jobQueue := range c.GPUJobQueues {
		gpu2JobQueueLength[jobQueue.GPU()] = len(jobQueue.Jobs())
	}
	exposed := &struct {
		Now Time
		GPU2JobQueueLength map[*GPU]int
	} {
		c.Now(), gpu2JobQueueLength,
	}
	return exposed
}