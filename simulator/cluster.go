package simulator

import (
	"DES-go/schedulers/types"
	"DES-go/util"
	"math"
	"sort"
	"sync"
)

type Cluster struct {
	gpuType2CountConfig map[types.GPUType]int
	gpus                map[types.GPUType][]types.GPU
	gpuTypes            []types.GPUType
	timer               types.Time
	gpuJobQueues        map[types.GPUID]*GPUJobQueue
}

func (c *Cluster) InitJob(jobMeta types.JobMeta) types.Job {
	return NewJob(jobMeta.JobName())
}

func (c *Cluster) GPUJobQueues() map[types.GPUID]types.GPUJobQueue {
	res := make(map[types.GPUID]types.GPUJobQueue)
	for k, v := range c.gpuJobQueues {
		res[k] = v
	}
	return res
}

func (c *Cluster) EmptyGPUJobQueues() []types.GPUJobQueue {
	emptyJobQueues := make([]types.GPUJobQueue, 0, len(c.gpuJobQueues))
	for _, jobQueue := range c.gpuJobQueues {
		if len(jobQueue.Jobs()) == 0 {
			emptyJobQueues = append(emptyJobQueues, jobQueue)
		}
	}
	return emptyJobQueues
}

func (c *Cluster) GPU(gpuID types.GPUID) types.GPU {
	return c.gpuJobQueues[gpuID].GPU()
}

func NewCluster(gpuType2CountConfig map[types.GPUType]int) *Cluster {
	gpus := make(map[types.GPUType][]types.GPU)
	gpuID := types.GPUID(0)
	gpuTypeStrings := make([]string, 0, len(gpuType2CountConfig))
	for gpuType := range gpuType2CountConfig {
		gpuTypeStrings = append(gpuTypeStrings, string(gpuType))
	}
	sort.Strings(gpuTypeStrings)
	gpuTypes := make([]types.GPUType, 0, len(gpuTypeStrings))
	for _, gpuTypeStr := range gpuTypeStrings {
		gpuType := types.GPUType(gpuTypeStr)
		gpuTypes = append(gpuTypes, gpuType)
		count := gpuType2CountConfig[gpuType]
		l := make([]types.GPU, 0, count)
		for i := 0; i < count; i++ {
			l = append(l, NewGPU(gpuID, gpuType))
			gpuID++
		}
		gpus[gpuType] = l
	}

	gpuJobQueues := make(map[types.GPUID]*GPUJobQueue)
	for _, gpuList := range gpus {
		for _, gpu := range gpuList {
			gpuJobQueues[gpu.ID()] = NewGPUJobQueue(gpu)
		}
	}

	return &Cluster{
		gpuType2CountConfig: gpuType2CountConfig,
		gpuTypes:            gpuTypes,
		gpus:                gpus,
		timer:               types.Time(-1),
		gpuJobQueues:        gpuJobQueues,
	}
}

func (c *Cluster) GPUs() map[types.GPUType][]types.GPU {
	return c.gpus
}

func (c *Cluster) GPUTypes() []types.GPUType {
	return c.gpuTypes
}

func (c *Cluster) startServe() {
	c.timer = 0
}

func (c *Cluster) isServing() bool {
	return c.timer != -1
}

func (c *Cluster) Now() types.Time {
	return c.timer
}

func (c *Cluster) CurrRunningJob(gpuID types.GPUID) types.Job {
	jobs := c.GPUJobQueues()[gpuID].Jobs()
	if len(jobs) > 0 && jobs[0].IsRunning() {
		return jobs[0]
	}
	return nil
}

func (c *Cluster) passDuration(duration types.Duration) []*Job {
	if !c.isServing() {
		panic("Cluster passDuration called when is not serving")
	}
	fromTime := c.timer
	c.timer += types.Time(duration)
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

func (c *Cluster) ClosestTimeToFinishAnyJob() types.Time {
	res := math.Inf(1)
	for _, q := range c.gpuJobQueues {
		res = math.Min(float64(q.FirstJobRemainingDuration()), res)
	}
	return types.Time(res)
}

func (c *Cluster) PrettyExpose() interface{} {
	gpu2JobQueueLength := make(map[types.GPU]int)
	for _, jobQueue := range c.gpuJobQueues {
		gpu2JobQueueLength[jobQueue.GPU()] = len(jobQueue.Jobs())
	}
	exposed := &struct {
		Now                types.Time
		GPU2JobQueueLength map[types.GPU]int
	}{
		c.Now(), gpu2JobQueueLength,
	}
	return exposed
}
