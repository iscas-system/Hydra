package simulator

import (
	"DES-go/schedulers/types"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
)

var cacheJobDuration = &sync.Map{}

type TimeRange struct {
	start types.Time
	end   types.Time
}

func (t *TimeRange) Start() types.Time {
	return t.start
}

func (t *TimeRange) End() types.Time {
	return t.end
}

func newTimeRange(start types.Time, end types.Time) *TimeRange {
	return &TimeRange{start: start, end: end}
}

func (t TimeRange) Runtime() types.Duration {
	return types.Duration(t.end - t.start)
}

type JobExecutionRange struct {
	gpu               types.GPU
	jobName           types.JobName
	timeRange         *TimeRange
	completenessRatio float64
}

func (jer *JobExecutionRange) GPU() types.GPU {
	return jer.gpu
}

func (jer *JobExecutionRange) JobName() types.JobName {
	return jer.jobName
}

func (jer *JobExecutionRange) TimeRange() types.TimeRange {
	return jer.timeRange
}

func (jer *JobExecutionRange) CompletenessRatio() float64 {
	return jer.completenessRatio
}

func newJobExecutionRange(gpu types.GPU, jobName types.JobName, timeRange *TimeRange) *JobExecutionRange {
	r := &JobExecutionRange{gpu: gpu, jobName: jobName, timeRange: timeRange}
	r.resetCompletenessRatio()
	return r
}

func (jer *JobExecutionRange) modifyTimeRange(start *types.Time, end *types.Time) {
	if jer.timeRange == nil {
		panic("modifyTimeRange jer.timeRange is nil")
	}
	if start != nil {
		jer.timeRange.start = *start
	}
	if end != nil {
		jer.timeRange.end = *end
	}
	jer.resetCompletenessRatio()
}

func (jer *JobExecutionRange) resetCompletenessRatio() float64 {
	jer.completenessRatio = float64(jer.timeRange.Runtime() / getDataSource().Duration(jer.jobName, jer.gpu.Type()))
	return jer.completenessRatio
}

type JobExecutionDetail struct {
	jobName         types.JobName
	executionRanges map[types.GPU][]types.JobExecutionRange
}

func newJobExecutionDetail(jobName types.JobName) *JobExecutionDetail {
	return &JobExecutionDetail{jobName: jobName}
}

func (jed *JobExecutionDetail) addExecutionRange(gpu types.GPU, timeRange *TimeRange) {
	if jed.executionRanges == nil {
		jed.executionRanges = make(map[types.GPU][]types.JobExecutionRange)
	}
	if _, ok := jed.executionRanges[gpu]; !ok {
		jed.executionRanges[gpu] = make([]types.JobExecutionRange, 0)
	}

	// In case that the last execution range is closely jointed with new execution range. Combine them.
	if len(jed.executionRanges[gpu]) > 0 {
		lastExecutionRange := jed.executionRanges[gpu][len(jed.executionRanges[gpu])-1]
		if math.Abs(float64(lastExecutionRange.TimeRange().End()-timeRange.Start())) < 1e-6 {
			(lastExecutionRange.(*JobExecutionRange)).modifyTimeRange(nil, &timeRange.end)
			return
		}
	}
	jed.executionRanges[gpu] = append(jed.executionRanges[gpu], newJobExecutionRange(gpu, jed.jobName, timeRange))
}

func (jed *JobExecutionDetail) SumRuntimeOnGPUs() types.Duration {
	if jed.executionRanges == nil {
		return 0
	}
	sum := types.Duration(0.)
	for _, rs := range jed.executionRanges {
		for _, r := range rs {
			sum += r.TimeRange().Runtime()
		}
	}
	return sum
}

func (jed *JobExecutionDetail) ExecutionRanges() map[types.GPU][]types.JobExecutionRange {
	return jed.executionRanges
}

type Job struct {
	jobName             types.JobName
	executionDetail     *JobExecutionDetail
	firstExecutionTime  types.Time
	finishExecutionTime types.Time
	remainingRatio      float64
	isRunning           bool
}

func (j *Job) JobName() types.JobName {
	return j.jobName
}

func (j *Job) ExecutionDetail() types.JobExecutionDetail {
	return j.executionDetail
}

func (j *Job) FirstExecutionTime() types.Time {
	return j.firstExecutionTime
}

func (j *Job) FinishExecutionTime() types.Time {
	return j.finishExecutionTime
}

func (j *Job) RemainingRatio() float64 {
	return j.remainingRatio
}

func (j *Job) QueueDelay() types.Duration {
	return types.Duration(j.JCT()) - j.ActualRuntimeOnGPUs()
}

func (j *Job) JobMeta() types.JobMeta {
	return getDataSource().JobMeta(j.JobName())
}

func (j *Job) HasDDL() bool {
	return !math.IsInf(float64(j.JobMeta().DDL()), 1)
}

func (j *Job) PrettyExpose() interface{} {
	return struct {
		types.Job
		types.JobMeta
	}{
		j, j.JobMeta(),
	}
}

func NewJob(jobName types.JobName) *Job {
	return &Job{
		jobName:             jobName,
		firstExecutionTime:  types.Time(-1),
		finishExecutionTime: types.Time(-1),
		remainingRatio:      1.,
	}
}

func (j *Job) IsRunning() bool {
	return j.isRunning
}

func (j *Job) setNotRunning() {
	j.isRunning = false
}

func (j *Job) executesFor(gpu types.GPU, fromTime types.Time, executesDur types.Duration) {
	if j.remainingRatio <= 0. {
		panic("executesFor j.remainingRatio <= 0.")
	}
	fullDurOnGPU := getDataSource().Duration(j.jobName, gpu.Type())
	remainingDuration := types.Duration(j.remainingRatio * float64(fullDurOnGPU))
	if j.firstExecutionTime == -1 {
		j.firstExecutionTime = fromTime
		j.executionDetail = newJobExecutionDetail(j.jobName)
	}
	if j.remainingRatio-float64(executesDur/fullDurOnGPU) <= 0. {
		// finished this job
		j.isRunning = false
		newExecutionTimeRange := newTimeRange(fromTime, fromTime+types.Time(remainingDuration))
		j.executionDetail.addExecutionRange(gpu, newExecutionTimeRange)
		j.remainingRatio = 0.
		j.finishExecutionTime = newExecutionTimeRange.end
	} else {
		// current job is not finished
		// set is_running
		j.isRunning = true
		newExecutionTimeRange := newTimeRange(fromTime, fromTime+types.Time(executesDur))
		j.executionDetail.addExecutionRange(gpu, newExecutionTimeRange)
		j.remainingRatio -= float64(executesDur / fullDurOnGPU)
		if j.remainingRatio <= 0. {
			panic(fmt.Sprintf("j.remainingRatio <= 0. remainingRatio == %f", j.remainingRatio))
		}
	}
}

func hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func (j *Job) RemainingDuration(gpuType types.GPUType) types.Duration {
	fullDurOnGPU := getDataSource().Duration(j.jobName, gpuType)
	var flag = 1.
	if flag == 0. || simulating {
		return types.Duration(j.remainingRatio * float64(fullDurOnGPU))
	}
	jobOnGPU := string(j.JobName()) + string(gpuType)
	cache, ok := cacheJobDuration.Load(jobOnGPU)
	if ok {
		return types.Duration(j.remainingRatio) * cache.(types.Duration)
	} else {
		strHash := hash(jobOnGPU)
		rand.Seed(int64(strHash))
		var flag = -1.
		var percent float64
		if flag == 1. {
			f := rand.Float64()
			if f < 0.2 {
				percent = -rand.Float64() * 0.1
			} else if rand.Float64() < 0.8 {
				percent = rand.Float64() * 0.15
			} else {
				percent = rand.Float64() * 0.23
			}
		} else {
			f := rand.Float64()
			if f < 0.2 {
				percent = -rand.Float64() * 0.1
			} else if rand.Float64() < 0.8 {
				percent = rand.Float64() * 0.1
			} else {
				percent = rand.Float64() * 0.2
			}
		}
		finalDur := types.Duration(float64(fullDurOnGPU) + percent * flag * float64(fullDurOnGPU))
		cacheJobDuration.Store(jobOnGPU, finalDur)
		return types.Duration(j.remainingRatio) * finalDur
	}
	//jobName := j.JobName()
	//if jobName == "8014d6c5e39fc326e14c8678" {
	//	print()
	//}
	//strHash := hash(string(jobName))
	//rand.Seed(int64(strHash))
	//var flag = 1.
	//var percent float64
	//if rand.Float64() < 0.8 {
	// 	5%
		//percent = rand.Float64() * 0.05
	//} else {
	//	20%
		//percent = rand.Float64() * 0.1
	//}
	//if !simulating {
	//	fullDurOnGPU = types.Duration(float64(fullDurOnGPU) + percent * flag * float64(fullDurOnGPU))
	//}
	//return types.Duration(j.remainingRatio * float64(fullDurOnGPU))
}

func (j *Job) ActualRuntimeOnGPUs() types.Duration {
	return j.executionDetail.SumRuntimeOnGPUs()
}

func (j *Job) JCT() types.Time {
	if j.finishExecutionTime == -1 {
		return -1
	}
	return j.finishExecutionTime - getDataSource().SubmitTime(j.jobName)
}

func (j *Job) Violation() (bool, types.Duration) {
	if j.finishExecutionTime == -1 {
		return false, -1
	}
	violatesDuration := math.Max(float64(j.finishExecutionTime-getDataSource().DDL(j.jobName)), 0.)
	return violatesDuration > 0., types.Duration(violatesDuration)
}

func (j *Job) IsFinished() bool {
	return j.remainingRatio <= 0.
}
