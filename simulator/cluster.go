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
	Timer        Time
	gpuJobQueues map[GPUID]*GPUJobQueue
}

func (c *Cluster) GPUJobQueues() map[GPUID]*GPUJobQueue {
	return c.gpuJobQueues
}

func (c *Cluster) GPU(gpuID GPUID) *GPU {
	return c.gpuJobQueues[gpuID].GPU()
}

func newCluster(gpuType2CountConfig map[GPUType]int) *Cluster {
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
		gpuJobQueues:        gpuJobQueues,
	}
}

func (c *Cluster) GPUs() map[GPUType][]*GPU {
	return c.gpus
}

func (c *Cluster) startServe() {
	c.Timer = 0
}

func (c *Cluster) IsServing() bool {
	return c.Timer != -1
}

func (c *Cluster) Now() Time {
	return c.Timer
}

func (c *Cluster) passDuration(duration Duration) []*Job {
	if !c.IsServing() {
		panic("Cluster passDuration called when is not serving")
	}
	fromTime := c.Timer
	c.Timer += Time(duration)
	newlyFinishedJobs := make([]*Job, 0)
	wg := &sync.WaitGroup{}
	finishedJobsSlice := make([][]*Job, len(c.gpuJobQueues))

	idx := 0
	for _, queue := range c.gpuJobQueues {
		queue := queue
		util.GoWithWG(wg, idx, func(idx int) {
			finishedJobsSlice[idx] = queue.passDuration(fromTime, duration)
		})
		idx++
	}
	wg.Wait()

	for _, jobs := range finishedJobsSlice {
		newlyFinishedJobs = append(newlyFinishedJobs, jobs...)
	}

	return newlyFinishedJobs
}

func (c *Cluster) ClosestTimeToFinishAnyJob() Time {
	res := math.Inf(1)
	for _, q := range c.gpuJobQueues {
		res = math.Min(float64(q.FirstJobRemainingDuration()), res)
	}
	return Time(res)
}

func (c *Cluster) PrettyExpose() interface{} {
	gpu2JobQueueLength := make(map[*GPU]int)
	for _, jobQueue := range c.gpuJobQueues {
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