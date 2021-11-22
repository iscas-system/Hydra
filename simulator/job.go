package simulator

import (
	"fmt"
	"math"
)

type TimeRange struct {
	start Time
	end   Time
}

func (t *TimeRange) Start() Time {
	return t.start
}

func (t *TimeRange) End() Time {
	return t.end
}

func newTimeRange(start Time, end Time) *TimeRange {
	return &TimeRange{start: start, end: end}
}

func (t TimeRange) Runtime() Duration {
	return Duration(t.end - t.start)
}

type JobExecutionRange struct {
	gpu               *GPU
	jobName           JobName
	timeRange         *TimeRange
	completenessRatio float64
}

func (jer *JobExecutionRange) Gpu() *GPU {
	return jer.gpu
}

func (jer *JobExecutionRange) Clone() *JobExecutionRange {
	return &JobExecutionRange{
		gpu:     jer.gpu,
		jobName: jer.jobName,
		timeRange: &TimeRange{
			start: jer.timeRange.start,
			end:   jer.timeRange.end,
		},
		completenessRatio: jer.completenessRatio,
	}
}

func newJobExecutionRange(gpu *GPU, jobName JobName, timeRange *TimeRange) *JobExecutionRange {
	r := &JobExecutionRange{gpu: gpu, jobName: jobName, timeRange: timeRange}
	r.resetCompletenessRatio()
	return r
}

func (jer *JobExecutionRange) modifyTimeRange(start *Time, end *Time) {
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

func (jer *JobExecutionRange) CompletenessRatio() float64 {
	return jer.completenessRatio
}

type JobExecutionDetail struct {
	jobName         JobName
	executionRanges map[*GPU][]*JobExecutionRange
}

func newJobExecutionDetail(jobName JobName) *JobExecutionDetail {
	return &JobExecutionDetail{jobName: jobName}
}

func (jed *JobExecutionDetail) Clone() *JobExecutionDetail {
	rs := make(map[*GPU][]*JobExecutionRange)
	for k, v := range jed.executionRanges {
		cv := make([]*JobExecutionRange, 0, len(v))
		for _, e := range v {
			cv = append(cv, e.Clone())
		}
		rs[k] = cv
	}
	return &JobExecutionDetail{
		jobName:         jed.jobName,
		executionRanges: rs,
	}
}

func (jed *JobExecutionDetail) addExecutionRange(gpu *GPU, timeRange *TimeRange) {
	if jed.executionRanges == nil {
		jed.executionRanges = make(map[*GPU][]*JobExecutionRange)
	}
	if _, ok := jed.executionRanges[gpu]; !ok {
		jed.executionRanges[gpu] = make([]*JobExecutionRange, 0)
	}

	// In case that the last execution range is closely jointed with new execution range. Combine them.
	if len(jed.executionRanges[gpu]) > 0 {
		lastExecutionRange := jed.executionRanges[gpu][len(jed.executionRanges[gpu])-1]
		if math.Abs(float64(lastExecutionRange.timeRange.end-timeRange.start)) < 1e-6 {
			lastExecutionRange.modifyTimeRange(nil, &timeRange.end)
			return
		}
	}
	jed.executionRanges[gpu] = append(jed.executionRanges[gpu], newJobExecutionRange(gpu, jed.jobName, timeRange))
}

func (jed *JobExecutionDetail) SumRuntimeOnGPUs() Duration {
	if jed.executionRanges == nil {
		return 0
	}
	sum := Duration(0.)
	for _, rs := range jed.executionRanges {
		for _, r := range rs {
			sum += r.timeRange.Runtime()
		}
	}
	return sum
}

type Job struct {
	jobName             JobName
	executionDetail     *JobExecutionDetail
	firstExecutionTime  Time
	finishExecutionTime Time
	remainingRatio      float64
	isRunning           bool
}

func (j *Job) JobName() JobName {
	return j.jobName
}

func (j *Job) ExecutionDetail() *JobExecutionDetail {
	return j.executionDetail
}

func (j *Job) FirstExecutionTime() Time {
	return j.firstExecutionTime
}

func (j *Job) FinishExecutionTime() Time {
	return j.finishExecutionTime
}

func (j *Job) RemainingRatio() float64 {
	return j.remainingRatio
}

func (j *Job) QueueDelay() Duration {
	return j.JCT() - j.ActualRuntimeOnGPUs()
}

func (j *Job) JobMeta() *JobMeta {
	return getDataSource().JobMeta(j.JobName())
}

func (j *Job) PrettyExpose() interface{} {
	return struct {
		*Job
		*JobMeta
	}{
		j, j.JobMeta(),
	}
}

func NewJob(jobName JobName) *Job {
	return &Job{
		jobName:             jobName,
		firstExecutionTime:  Time(-1),
		finishExecutionTime: Time(-1),
		remainingRatio:      1.,
	}
}

func (j *Job) IsRunning() bool {
	return j.isRunning
}

func (j *Job) setNotRunning() {
	j.isRunning = false
}

func (j *Job) executesFor(gpu *GPU, fromTime Time, executesDur Duration) {
	if j.remainingRatio <= 0. {
		panic("executesFor j.remainingRatio <= 0.")
	}
	fullDurOnGPU := getDataSource().Duration(j.jobName, gpu.Type())
	remainingDuration := Duration(j.remainingRatio * float64(fullDurOnGPU))
	if j.firstExecutionTime == -1 {
		j.firstExecutionTime = fromTime
		j.executionDetail = newJobExecutionDetail(j.jobName)
	}
	if j.remainingRatio-float64(executesDur/fullDurOnGPU) <= 0. {
		// finished this job
		j.isRunning = false
		newExecutionTimeRange := newTimeRange(fromTime, fromTime+Time(remainingDuration))
		j.executionDetail.addExecutionRange(gpu, newExecutionTimeRange)
		j.remainingRatio = 0.
		j.finishExecutionTime = newExecutionTimeRange.end
	} else {
		// current job is not finished
		// set is_running
		j.isRunning = true
		newExecutionTimeRange := newTimeRange(fromTime, fromTime+Time(executesDur))
		j.executionDetail.addExecutionRange(gpu, newExecutionTimeRange)
		j.remainingRatio -= float64(executesDur / fullDurOnGPU)
		if j.remainingRatio <= 0. {
			panic(fmt.Sprintf("j.remainingRatio <= 0. remainingRatio == %f", j.remainingRatio))
		}
	}
}

func (j *Job) RemainingDuration(gpuType GPUType) Duration {
	fullDurOnGPU := getDataSource().Duration(j.jobName, gpuType)
	return Duration(j.remainingRatio * float64(fullDurOnGPU))
}

func (j *Job) ActualRuntimeOnGPUs() Duration {
	return j.executionDetail.SumRuntimeOnGPUs()
}

func (j *Job) JCT() Duration {
	if j.finishExecutionTime == -1 {
		return -1
	}
	return Duration(j.finishExecutionTime - getDataSource().SubmitTime(j.jobName))
}

func (j *Job) Violation() (bool, Duration) {
	if j.finishExecutionTime == -1 {
		return false, -1
	}
	violatesDuration := math.Max(float64(j.finishExecutionTime-getDataSource().DDL(j.jobName)), 0.)
	return violatesDuration > 0., Duration(violatesDuration)
}

func (j *Job) Clone() *Job {
	cloned := &Job{
		jobName:             j.jobName,
		executionDetail:     j.executionDetail.Clone(),
		firstExecutionTime:  j.firstExecutionTime,
		finishExecutionTime: j.finishExecutionTime,
		remainingRatio:      j.remainingRatio,
		isRunning:           j.isRunning,
	}
	return cloned
}

func (j *Job) IsFinished() bool {
	return j.remainingRatio <= 0.
}
