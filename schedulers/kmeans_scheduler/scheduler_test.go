package kmeans_scheduler

import (
	"DES-go/simulator"
	"testing"
)

func Test1(t *testing.T) {
	scheduler := New(WithScheme(
		&SimpleOneShotScheme{
			Preemptive: false,
		}),
		WithDistanceAlgoArgs(&AlgoBranchAndBoundArgs{LCStandard: BranchAndBoundLCStandardPartialCost}),
		WithDDLCostType(DDLCostTypeStrict),
	)
	simu := simulator.NewSimulator(scheduler,
		simulator.WithOptionFmtPrintLevel(simulator.ShortMsgPrint),
		simulator.WithOptionLogEnabled(true),
		simulator.WithOptionLogPath("/Users/purchaser/go/src/DES-go/logs"),
		simulator.WithOptionGPUType2Count(map[simulator.GPUType]int{
			"V100": 1,
			"T4":   1,
		}))
	simulator.SetDataSource([]*simulator.JobMeta{
		simulator.NewJobMeta("job1", 0, 12, map[simulator.GPUType]simulator.Duration{"V100": 5, "T4": 10}),
		simulator.NewJobMeta("job2", 0, 7, map[simulator.GPUType]simulator.Duration{"V100": 6, "T4": 12}),
		simulator.NewJobMeta("job3", 0, 6, map[simulator.GPUType]simulator.Duration{"V100": 3, "T4": 5}),
	})
	simu.Start()
	//scheduler.insertJobs2Waiting(simulator.NewJob("job1"), simulator.NewJob("job2"), simulator.NewJob("job3"))
	//scheduler.doSimpleOneShotSchedule(&SimpleOneShotScheme{
	//	Preemptive:      false,
	//	PreemptiveCycle: 0,
	//})

}

func Test2(t *testing.T) {
	a := []int{1, 2}
	b := make([]int, 0, 3)
	copy(b, a)
	t.Log(b)
}
