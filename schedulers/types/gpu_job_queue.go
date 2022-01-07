package types

type GPUJobQueue interface {
	GPU() GPU
	Jobs() []Job
	SetJobs(jobs ...Job)
	ClearQueue() []Job
	FirstJobRemainingDuration() Duration
}
