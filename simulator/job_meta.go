package simulator

import "DES-go/schedulers/types"

type JobMeta struct {
	jobName    types.JobName
	submitTime types.Time
	ddl        types.Time
	durations  map[types.GPUType]types.Duration
}

func (m *JobMeta) JobName() types.JobName {
	return m.jobName
}

func (m *JobMeta) DDL() types.Time {
	return m.ddl
}

func (m *JobMeta) Durations() map[types.GPUType]types.Duration {
	return m.durations
}

func (m *JobMeta) Duration(gpu types.GPU) types.Duration {
	return m.durations[gpu.Type()]
}

func NewJobMeta(jobName types.JobName, submitTime types.Time, ddl types.Time, durations map[types.GPUType]types.Duration) *JobMeta {
	return &JobMeta{
		jobName:    jobName,
		submitTime: submitTime,
		ddl:        ddl,
		durations:  durations,
	}
}

func (m *JobMeta) SubmitTime() types.Time {
	return m.submitTime
}
