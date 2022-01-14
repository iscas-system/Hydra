package simulator

import (
	"DES-go/schedulers/types"
	"DES-go/util"
	"fmt"
	"math"
)

type Simulator struct {
	opts      *Options
	scheduler types.Scheduler
	cluster   *Cluster
	logger    *logger

	recordedFinishedJobs []*Job
}

func NewSimulator(scheduler types.Scheduler, setOpts ...SetOption) *Simulator {
	opts := defaultOptions

	for _, setOpt := range setOpts {
		setOpt(opts)
	}
	if opts.dataSourceCSVPath != "" {
		initDataSource(opts.dataSourceCSVPath)
	}

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
	s.cluster.startServe()
	s.scheduler.SetCluster(s.cluster)
	getDataSource().IterBySubmitTime(func(indices []int, metas []types.JobMeta) {
		submitTime := metas[0].SubmitTime()
		for _, meta := range metas {
			if meta.SubmitTime() != submitTime {
				panic("getDataSource().IterBySubmitTime metas' submit times are different.")
			}
		}

		if float64(submitTime-s.cluster.Now()) < -float64(s.opts.minDurationPassInterval) {
			panic(fmt.Sprintf("meta.submitTime() = %v - s.cluster.Now() = %v) >= -float64(s.opts.minDurationPassInterval = %v)", submitTime, s.cluster.Now(), s.opts.minDurationPassInterval))
		}
		for s.cluster.Now() < submitTime {
			passDuration := submitTime - s.cluster.Now()
			s.passDuration(types.Duration(passDuration), false)
		}
		s.emitEvent(types.NewScheduleEventJobsArrived(metas))
	})
	s.passDuration(0, true)
	s.logMetrics()
	s.logger.Exit()
}

func (s *Simulator) passDuration(duration types.Duration, noMoreNewSubmits bool) {
	currTime := s.cluster.Now()
	targetTime := currTime + types.Time(duration)
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
		partialDuration := types.Duration(math.Min(math.Max(possibleNextEventTime, float64(s.opts.minDurationPassInterval)), float64(targetTime-currTime)))
		finishedJobs := s.cluster.passDuration(partialDuration)
		fmt.Printf("finishedJobs len=[%d], all Finished len=[%d]", len(finishedJobs), len(s.recordedFinishedJobs))
		s.logTimePassed(partialDuration)
		currTime += types.Time(partialDuration)
		s.recordedFinishedJobs = append(s.recordedFinishedJobs, finishedJobs...)
		s.logger.ReceiveFinishedJobs(finishedJobs)
		s.emitEvent(types.NewScheduleEventDurationPassed(partialDuration))
		if len(finishedJobs) > 0 {
			s.emitEvent(types.NewScheduleEventJobsFinished(s.transformJobs(finishedJobs)))
		}
	}
}

func (s *Simulator) transformJobs(jobs []*Job) []types.Job {
	res := make([]types.Job, 0, len(jobs))
	for _, job := range jobs {
		res = append(res, job)
	}
	return res
}

func (s *Simulator) logTimePassed(duration types.Duration) {
	allInfo := util.PrettyF("\nTime Passed: %f seconds, finished jobs count: %d. \ncluster info: \n%# v.\n", float64(duration), len(s.recordedFinishedJobs), s.cluster)
	if s.opts.formatPrintLevel == AllFormatPrint {
		fmt.Printf(allInfo)
	} else if s.opts.formatPrintLevel == ShortMsgPrint {
		shortInfo := util.PrettyF("\nTime Passed: %f seconds, finished jobs count: %d.\n", float64(duration), len(s.recordedFinishedJobs))
		fmt.Printf(shortInfo)
	} else if s.opts.formatPrintLevel == NoFormatPrint {
		// pass.
	}
	s.logger.ReceiveStringLog(allInfo)
}

// TODO Deprecated. 使用metrics包替代。logger仅用来记录模拟器的详细调度过程。
func (s *Simulator) logMetrics() {
	violationCount, avgViolationDelay := Violation(s.recordedFinishedJobs)
	metrics := util.PrettyF("simulation completed, "+
		"scheduler = [%s], "+
		"finished job count = [%d], "+
		"avg jct = [%f], "+
		"violated job count = [%d], "+
		"avg violate delay = [%f] "+
		"avg queuing delay = [%f] "+
		"\n",
		s.scheduler.Name(), len(s.recordedFinishedJobs), AvgJCT(s.recordedFinishedJobs), violationCount, avgViolationDelay, AvgQueuingDelay(s.recordedFinishedJobs))

	fmt.Println(metrics)
	s.logger.ReceiveStringLog(metrics)
}

func (s *Simulator) emitEvent(event types.ScheduleEvent) {
	s.scheduler.OnScheduleEvent(event)
}
