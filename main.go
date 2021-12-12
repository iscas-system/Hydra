package main

import (
	"DES-go/schedulers"
	"DES-go/schedulers/kmeans_scheduler"
	"DES-go/simulator"
)

func main() {
	// scheduler := initDummyScheduler()
	//scheduler := initSJFScheduler()
	scheduler := initKMeansScheduler()
	simu := simulator.NewSimulator(scheduler,
		simulator.WithOptionFmtPrintLevel(simulator.ShortMsgPrint),
		simulator.WithOptionDataSourceCSVPath("/Users/yangchen/Projects/Graduate/DES-go/cases/case_600.csv"),
		simulator.WithOptionLogEnabled(true),
		simulator.WithOptionLogPath("/Users/yangchen/Projects/Graduate/DES-go/logs"),
		simulator.WithOptionGPUType2Count(map[simulator.GPUType]int{
			"V100": 20,
			"P100": 20,
			"T4":   20,
		}))
	simu.Start()
}

func initDummyScheduler() simulator.Scheduler {
	return schedulers.NewDummyScheduler()
}

func initSJFScheduler() simulator.Scheduler {
	return schedulers.NewSJFScheduler(false)
}

func initKMeansScheduler() simulator.Scheduler {
	return kmeans_scheduler.New(
		kmeans_scheduler.WithScheme(&kmeans_scheduler.SimpleOneShotScheme{
			Preemptive: false,
		}),
		kmeans_scheduler.WithDistanceAlgoArgs(&kmeans_scheduler.DistanceAlgoMinCostArgs{
			MinCostAlgoArgs: &kmeans_scheduler.MinCostByBranchAndBoundArgs{
				LCStandard: kmeans_scheduler.BranchAndBoundLCStandardPredictCost,
			}}),
		kmeans_scheduler.WithDDLCostType(kmeans_scheduler.DDLCostTypeStrict),
	)
}
