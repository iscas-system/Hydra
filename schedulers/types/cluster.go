package types

import "DES-go/util"

type Cluster interface {
	GPUJobQueues() map[GPUID]GPUJobQueue
	EmptyGPUJobQueues() []GPUJobQueue
	GPU(gpuID GPUID) GPU
	GPUs() map[GPUType][]GPU
	GPUTypes() []GPUType
	Now() Time
	CurrRunningJob(gpuID GPUID) Job
	ClosestTimeToFinishAnyJob() Time
	InitJob(jobMeta JobMeta) Job
	util.PrettyExpose
}
