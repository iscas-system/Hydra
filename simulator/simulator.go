package simulator

import (
	"DES-go/util"
	"fmt"
	"math"
)

type Simulator struct {
	opts      *Options
	scheduler Scheduler
	cluster   *Cluster
	logger    *Logger

	recordedFinishedJobs []*Job
}

func NewSimulator(scheduler Scheduler, setOpts ...SetOption) *Simulator {
	opts := defaultOptions

	for _, setOpt := range setOpts {
		setOpt(opts)
	}
	InitDataSource(opts.dataSourceCSVPath)

	logger := NewLogger(opts.logEnabled, opts.logDirPath)
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
	GetDataSource().IterBySubmitTime(func(indices []int, metas []*JobMeta) {
		fmt.Printf("indices=[%v]", indices)
		submitTime := metas[0].SubmitTime
		for _, meta := range metas {
			if meta.SubmitTime != submitTime {
				panic("GetDataSource().IterBySubmitTime metas' submit times are different.")
			}
		}

		if float64(submitTime-s.cluster.Now()) < -float64(s.opts.minDurationPassInterval) {
			panic(fmt.Sprintf("meta.SubmitTime() = %v - s.cluster.Now() = %v) >= -float64(s.opts.minDurationPassInterval = %v)", submitTime, s.cluster.Now(), s.opts.minDurationPassInterval))
		}
		for s.cluster.Now() < submitTime {
			passDuration := submitTime - s.cluster.Now()
			s.passDuration(Duration(passDuration), false)
		}
		s.emitEvent(NewScheduleEventJobsArrived(metas))
	})
	s.passDuration(0, true)
	s.logMetrics()
	s.logger.Exit()
}

func (s *Simulator) passDuration(duration Duration, noMoreNewSubmits bool) {
	currTime := s.cluster.Now()
	targetTime := currTime + Time(duration)
	if noMoreNewSubmits {
		targetTime = 1e38
	}
	for currTime < targetTime || noMoreNewSubmits {
		closestTimeToFinishAnyJob := s.cluster.ClosestTimeToFinishAnyJob()
		nextActiveScheduleTime := s.scheduler.NextActiveScheduleTime()
		// 如果调度器将不会进行主动调度，并且将来没有任务要完成，并且指定不会再有新的任务提交了，那么此时认为模拟结束了。
		if math.IsInf(float64(nextActiveScheduleTime), 1) &&
			math.IsInf(float64(closestTimeToFinishAnyJob), 1) &&
			noMoreNewSubmits {
			// All jobs done
			return
		}
		// calculate partial time.
		// in case some jobs finish very closely, use max() to specify a min interval.
		// targetTime - currTime is the upper limit.
		possibleNextEventTime := math.Min(float64(s.scheduler.NextActiveScheduleTime()), float64(closestTimeToFinishAnyJob))
		partialDuration := Duration(math.Min(math.Max(possibleNextEventTime, float64(s.opts.minDurationPassInterval)), float64(targetTime-currTime)))
		finishedJobs := s.cluster.PassDuration(partialDuration)
		fmt.Printf("finishedJobs len=[%d], all Finished len=[%d]", len(finishedJobs), len(s.recordedFinishedJobs))
		s.logTimePassed(partialDuration)
		currTime += Time(partialDuration)
		s.recordedFinishedJobs = append(s.recordedFinishedJobs, finishedJobs...)
		s.logger.ReceiveFinishedJobs(finishedJobs)
		s.emitEvent(NewScheduleEventDurationPassed(partialDuration))
		s.emitEvent(NewScheduleEventJobsFinished(finishedJobs))
	}
}

func (s *Simulator) logTimePassed(duration Duration) {
	allInfo := util.PrettyF("\nTimePassed for %f seconds, finished jobs count: %d. \ncluster info: \n%# v.\n", float64(duration), len(s.recordedFinishedJobs), s.cluster)
	if s.opts.formatPrintLevel == AllFormatPrint {
		fmt.Printf(allInfo)
	} else if s.opts.formatPrintLevel == ShortMsgPrint {
		shortInfo := util.PrettyF("\nTimePassed for %f seconds, finished jobs count: %d.\n", float64(duration), len(s.recordedFinishedJobs))
		fmt.Printf(shortInfo)
	} else if s.opts.formatPrintLevel == NoFormatPrint {
		// pass.
	}
	s.logger.ReceiveStringLog(allInfo)
}

func (s *Simulator) logMetrics() {
	violationCount, avgViolationDelay := MetricViolation(s.recordedFinishedJobs)
	metrics := util.PrettyF("simulation completed, scheduler = [%s], finished job count = [%d], avg jct = [%f], violated job count = [%d], avg violate delay = [%f]\n",
		s.scheduler, len(s.recordedFinishedJobs), AvgJCT(s.recordedFinishedJobs), violationCount, avgViolationDelay)

	fmt.Println(metrics)
	s.logger.ReceiveStringLog(metrics)
}

func (s *Simulator) emitEvent(event ScheduleEvent) {
	s.scheduler.OnScheduleEvent(event)
}
