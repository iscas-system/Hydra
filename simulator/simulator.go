package simulator

import (
	"context"
	"fmt"
	"log"
	"math"
)

type Simulator struct {
	opts      *Options
	scheduler Scheduler
	cluster   *Cluster
	logger    *Logger
	loggerCtx context.Context

	recordedFinishedJobs []*Job
}

func NewSimulator(scheduler Scheduler, setOpts ...SetOption) *Simulator {
	opts := defaultOptions

	for _, setOpt := range setOpts {
		setOpt(opts)
	}

	loggerCtx := context.Background()
	logger := NewLogger(loggerCtx, opts.logEnabled, opts.logDirPath)
	return &Simulator{
		scheduler:            scheduler,
		opts:                 opts,
		cluster:              NewCluster(opts.gpuType2Count),
		logger:               logger,
		recordedFinishedJobs: make([]*Job, 0),
	}
}

func (s *Simulator) Start() {
	s.cluster.StartServe()
	s.scheduler.SetCluster(s.cluster)
	GetDataSource().IterBySubmitTime(func(_ []int, metas []*JobMeta) {
		submitTime := metas[0].SubmitTime()
		for _, meta := range metas {
			if meta.SubmitTime() != submitTime {
				panic("GetDataSource().IterBySubmitTime metas' submit times are different.")
			}
		}

		if float64(submitTime-s.cluster.Now()) < -float64(s.opts.minDurationPassInterval) {
			panic(fmt.Sprintf("meta.SubmitTime() = %v - s.cluster.Now() = %v) >= -float64(s.opts.minDurationPassInterval = %v)", submitTime, s.cluster.Now(), s.opts.minDurationPassInterval))
		}
		for s.cluster.Now() < submitTime {
			passDuration := submitTime - s.cluster.Now()
			s.PassDuration(Duration(passDuration), false)
		}
		s.EmitEvent(NewScheduleEventJobsArrived(metas))
	})
	s.PassDuration(0, true)
	s.logMetrics()
	s.loggerCtx.Done()
}

func (s *Simulator) PassDuration(duration Duration, untilEnd bool) {
	currTime := s.cluster.Now()
	targetTime := currTime + Time(duration)
	if untilEnd {
		targetTime = 1e38
	}
	for currTime < targetTime || untilEnd {
		closestTimeToFinishAnyJob := s.cluster.ClosestTimeToFinishAnyJob()
		if math.IsInf(float64(closestTimeToFinishAnyJob), 1) && untilEnd {
			// All jobs done
			return
		}
		// calculate partial time.
		// in case some jobs finish very closely, use max() to specify a min interval.
		// targetTime - currTime is the upper limit.
		possibleNextEventTime := math.Min(float64(s.scheduler.NextActiveScheduleTime()), float64(closestTimeToFinishAnyJob))
		partialDuration := Duration(math.Min(math.Max(possibleNextEventTime, float64(s.opts.minDurationPassInterval)), float64(targetTime-currTime)))
		finishedJobs := s.cluster.PassDuration(partialDuration)
		s.logCurrCluster()
		currTime += Time(partialDuration)
		s.recordedFinishedJobs = append(s.recordedFinishedJobs, finishedJobs...)
		s.logger.ReceiveFinishedJobs(finishedJobs)
		s.EmitEvent(NewScheduleEventDurationPassed(partialDuration))
		s.EmitEvent(NewScheduleEventJobsFinished(finishedJobs))
	}
}

func (s *Simulator) logCurrCluster() {
	log.Printf("\ncluster info: %+v. \n finished jobs count: %d\n", s.cluster, len(s.recordedFinishedJobs))
}

func (s *Simulator) logMetrics() {
	violationCount, avgViolationDelay := MetricViolation(s.recordedFinishedJobs)
	metrics := fmt.Sprintf("simulation completed, scheduler = [%+v], finished job count = [%d], avg jct = [%f], violated job count = [%d], avg violate delay = [%f]\n",
		s.scheduler, len(s.recordedFinishedJobs), AvgJCT(s.recordedFinishedJobs), violationCount, avgViolationDelay)
	s.logger.ReceiveMetrics(metrics)
}

func (s *Simulator) EmitEvent(event ScheduleEvent) {
	s.scheduler.OnScheduleEvent(event)
}
