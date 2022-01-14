package metrics

type Report struct {
	CasePath string `json:"case_path"`
	SchedulerName string `json:"scheduler_name"`
	SchedulerInfo interface{} `json:"scheduler_info"`
	SchedulerRecord interface{} `json:"scheduler_record"`
	ClusterConfig *ClusterConfig `json:"cluster_config"`
	Execution *Execution `json:"execution"`
}

type GPU struct {
	Name string `json:"name"`
}

type ClusterConfig struct {
	GPUs map[*GPU]int `json:"GPUs"`
}

type Job struct {
	Name string `json:"name"`
	SubmitTime float64 `json:"submit_time"`
	FinishedTime float64 `json:"finished_time"`
	JCT float64 `json:"jct"`
	DDL float64 `json:"ddl"`
	Violated bool `json:"violated"`
	ViolatedDuration float64 `json:"violated_duration"`
	QueueDelayDuration float64 `json:"queue_delay_duration"`
	ExecutionRanges []*JobExecutionRange `json:"execution_ranges"`
}

type JobExecutionRange struct {
	GPU *GPU `json:"gpu"`
	StartTime float64 `json:"start_time"`
	End float64 `json:"end"`
	Runtime float64 `json:"runtime"`
}

type Execution struct {
	AverageJCT float64 `json:"average_jct"`
	AverageDDLViolationDuration float64 `json:"average_ddl_violation_duration"`
	DDLViolatedJobs []*Job `json:"ddl_violated_jobs"`
	DDLViolatedJobsCount int `json:"ddl_violated_jobs_count"`
	FinishedJobs []*Job `json:"finished_jobs"`
}
