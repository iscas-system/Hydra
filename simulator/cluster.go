package simulator

import (
	"DES-go/util"
	"fmt"
	"math"
	"sort"
	"sync"
)

type Cluster struct {
	gpuType2CountConfig map[GPUType]int
	gpus                map[GPUType][]*GPU
	gpuTypes     []GPUType
	timer        Time
	gpuJobQueues map[GPUID]*GPUJobQueue
}

func (c *Cluster) GPUJobQueues() map[GPUID]*GPUJobQueue {
	return c.gpuJobQueues
}

func (c *Cluster) EmptyGPUJobQueues() []*GPUJobQueue {
	emptyJobQueues := make([]*GPUJobQueue, 0, len(c.gpuJobQueues))
	for _, jobQueue := range c.gpuJobQueues {
		if len(jobQueue.Jobs()) == 0 {
			emptyJobQueues = append(emptyJobQueues, jobQueue)
		}
	}
	return emptyJobQueues
}

func (c *Cluster) GPU(gpuID GPUID) *GPU {
	return c.gpuJobQueues[gpuID].GPU()
}

func NewCluster(gpuType2CountConfig map[GPUType]int) *Cluster {
	gpus := make(map[GPUType][]*GPU)
	gpuID := GPUID(0)
	gpuTypeStrings := make([]string, 0, len(gpuType2CountConfig))
	for gpuType := range gpuType2CountConfig {
		gpuTypeStrings = append(gpuTypeStrings, string(gpuType))
	}
	sort.Strings(gpuTypeStrings)
	gpuTypes := make([]GPUType, 0, len(gpuTypeStrings))
	for _, gpuTypeStr := range gpuTypeStrings {
		gpuType := GPUType(gpuTypeStr)
		gpuTypes = append(gpuTypes, gpuType)
		count := gpuType2CountConfig[gpuType]
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
		gpuTypes:            gpuTypes,
		gpus:                gpus,
		timer:               Time(-1),
		gpuJobQueues:        gpuJobQueues,
	}
}

func (c *Cluster) GPUs() map[GPUType][]*GPU {
	return c.gpus
}

func (c *Cluster) GPUTypes() []GPUType {
	return c.gpuTypes
}

func (c *Cluster) startServe() {
	c.timer = 0
}

func (c *Cluster) IsServing() bool {
	return c.timer != -1
}

func (c *Cluster) Now() Time {
	return c.timer
}

func (c *Cluster) CurrRunningJob(gpuID GPUID) *Job {
	jobs := c.GPUJobQueues()[gpuID].Jobs()
	if len(jobs) > 0 && jobs[0].IsRunning() {
		return jobs[0]
	}
	return nil
}

func (c *Cluster) passDuration(duration Duration) []*Job {
	if !c.IsServing() {
		panic("Cluster passDuration called when is not serving")
	}
	fromTime := c.timer
	c.timer += Time(duration)
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
		Now                Time
		GPU2JobQueueLength map[*GPU]int
	}{
		c.Now(), gpu2JobQueueLength,
	}
	return exposed
}
