package simulator

type JobName string

type JobMeta struct {
	jobName    JobName
	submitTime Time
	ddl       Time
	durations map[GPUType]Duration
}

func (m *JobMeta) JobName() JobName {
	return m.jobName
}

func (m *JobMeta) DDL() Time {
	return m.ddl
}

func (m *JobMeta) Durations() map[GPUType]Duration {
	return m.durations
}

func (m *JobMeta) Duration(gpu *GPU) Duration {
	return m.durations[gpu.Type()]
}

func newJobMeta(jobName JobName, submitTime Time, ddl Time, durations map[GPUType]Duration) *JobMeta {
	return &JobMeta{
		jobName:    jobName,
		submitTime: submitTime,
		ddl:        ddl,
		durations:  durations,
	}
}

func (m *JobMeta) SubmitTime() Time {
	return m.submitTime
}

