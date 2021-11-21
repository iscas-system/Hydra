package simulator

type ScheduleEventType int

const (
	DurationPassed = ScheduleEventType(0)
	JobsArrived    = ScheduleEventType(1)
	JobsFinished   = ScheduleEventType(2)
)

type ScheduleEvent interface {
	GetEventType() ScheduleEventType
}

type ScheduleEventDurationPassed struct {
	duration Duration
}

func (s *ScheduleEventDurationPassed) Duration() Duration {
	return s.duration
}

func newScheduleEventDurationPassed(duration Duration) *ScheduleEventDurationPassed {
	return &ScheduleEventDurationPassed{duration: duration}
}

func (s *ScheduleEventDurationPassed) GetEventType() ScheduleEventType {
	return DurationPassed
}

type ScheduleEventJobsArrived struct {
	jobMetas []*JobMeta
}

func (s ScheduleEventJobsArrived) JobMetas() []*JobMeta {
	return s.jobMetas
}

func newScheduleEventJobsArrived(jobMetas []*JobMeta) *ScheduleEventJobsArrived {
	return &ScheduleEventJobsArrived{jobMetas: jobMetas}
}

func (s *ScheduleEventJobsArrived) GetEventType() ScheduleEventType {
	return JobsArrived
}

type ScheduleEventJobsFinished struct {
	jobs []*Job
}

func (s ScheduleEventJobsFinished) Jobs() []*Job {
	return s.jobs
}

func newScheduleEventJobsFinished(jobs []*Job) *ScheduleEventJobsFinished {
	return &ScheduleEventJobsFinished{jobs: jobs}
}

func (s *ScheduleEventJobsFinished) GetEventType() ScheduleEventType {
	return JobsFinished
}
