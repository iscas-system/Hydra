package simulator

type JobName string

type JobMeta struct {
	JobName    JobName `json:"job_name"`
	SubmitTime Time
	DDL        Time
	Durations  map[GPUType]Duration
}

func NewJobMeta(jobName JobName, submitTime Time, ddl Time, durations map[GPUType]Duration) *JobMeta {
	return &JobMeta{
		JobName:    jobName,
		SubmitTime: submitTime,
		DDL:        ddl,
		Durations:  durations,
	}
}
