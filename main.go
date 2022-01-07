package main

import (
	"DES-go/schedulers"
	"DES-go/schedulers/kmeans_scheduler"
	"DES-go/simulator"
)

func main() {
	// scheduler := initDummyScheduler()
	// scheduler := initSJFScheduler()
	scheduler := initKMeansScheduler()
	simu := simulator.NewSimulator(scheduler,
		simulator.WithOptionFmtPrintLevel(simulator.ShortMsgPrint),
		simulator.WithOptionDataSourceCSVPath("/Users/yangchen/Projects/Graduate/DES-go/cases/case_200_all_3.csv"),
		simulator.WithOptionLogEnabled(true),
		simulator.WithOptionLogPath("/tmp/logs"),
		simulator.WithOptionGPUType2Count(map[simulator.GPUType]int{
			"V100": 8,
			"P100": 8,
			"T4":   4,
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
		kmeans_scheduler.WithScheme(kmeans_scheduler.NewSimpleOneShotScheduleScheme(false, -1)),
		kmeans_scheduler.WithDistanceAlgo(kmeans_scheduler.NewMinCostDistanceAlgo(
			kmeans_scheduler.NewMinCostByBranchAndBoundAlgo(
				kmeans_scheduler.MinCostBranchAndBoundLCStandardPredictCost),
			kmeans_scheduler.NewSimpleAddCostSolverMaker(kmeans_scheduler.DDLCostTypeStrict, 1e20))),
	)
}