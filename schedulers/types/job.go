package types

import "DES-go/util"

type JobName string


type JobExecutionDetail interface {
	SumRuntimeOnGPUs() Duration
}

type JobExecutionRange interface {

}

type Job interface {
	JobName() JobName
	ExecutionDetail() JobExecutionDetail
	FirstExecutionTime() Time
	FinishExecutionTime() Time
	RemainingRatio() float64
	RemainingDuration(gpuType GPUType) Duration
	IsRunning() bool
	IsFinished() bool
	QueueDelay() Duration
	JobMeta() JobMeta
	Violation() (bool, Duration)
	JCT() Time
	ActualRuntimeOnGPUs() Duration
	util.PrettyExpose
}

type JobMeta interface {
	JobName() JobName
	DDL() Time
	Durations() map[GPUType]Duration
	Duration(gpu GPU) Duration
	SubmitTime() Time
}
