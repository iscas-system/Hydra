package main

import (
	"DES-go/metrics"
	"DES-go/schedulers"
	"DES-go/schedulers/allox_scheduler"
	"DES-go/schedulers/kmeans_scheduler"
	"DES-go/schedulers/kmeans_scheduler/cost"
	"DES-go/schedulers/types"
	"DES-go/simulator"
)

func main() {
	clusterConfig := map[types.GPUType]int{
		"V100": 10,
		"P100": 20,
		"T4":   30,
	}

	//casePath := "/Users/purchaser/go/src/DES-go/cases/case1.csv"
	//casePath := "/Users/purchaser/go/src/DES-go/cases/case_50_all.csv"
	//casePath := "/Users/purchaser/go/src/DES-go/cases/case_200_all.csv"
	//casePath := "/Users/purchaser/go/src/DES-go/cases/case_200_all_2.csv"
	//casePath := "/Users/purchaser/go/src/DES-go/cases/case_200_all_3.csv"
	casePath := "/Users/purchaser/go/src/DES-go/cases/case_500_all.csv"
	//casePath := "/Users/purchaser/go/src/DES-go/cases/case_500_long_all.csv"

	//scheduler := initDummyScheduler()
	//scheduler := initSJFScheduler()
	//scheduler := initEDFScheduler()
	//scheduler := initKMeansScheduler()
	scheduler := initAlloxScheduler()
	simu := simulator.NewSimulator(scheduler,
		simulator.WithOptionFmtPrintLevel(simulator.ShortMsgPrint),
		simulator.WithOptionDataSourceCSVPath(casePath),
		simulator.WithOptionLogEnabled(false),
		simulator.WithOptionGPUType2Count(clusterConfig))
	record := simu.Run()
	metrics.SaveSimulationReport("/Users/purchaser/go/src/DES-go/logs", record, &metrics.SimulationMetaConfig{
		CasePath:      casePath,
		ClusterConfig: clusterConfig,
	})
}

func initDummyScheduler() types.Scheduler {
	return schedulers.NewDummyScheduler()
}

func initSJFScheduler() types.Scheduler {
	return schedulers.NewSJFScheduler()
}

func initEDFScheduler() types.Scheduler {
	return schedulers.NewEDFScheduler()
}

func initAlloxScheduler() types.Scheduler {
	return allox_scheduler.NewAlloxScheduler(false)
}

func initKMeansScheduler() types.Scheduler {
	return kmeans_scheduler.New(
		//kmeans_scheduler.WithScheme(kmeans_scheduler.NewBasicScheduleScheme(true, false, -1, false)),
		kmeans_scheduler.WithScheme(kmeans_scheduler.NewBasicScheduleScheme(true, false, -1, true)),
		kmeans_scheduler.WithDistanceAlgo(kmeans_scheduler.NewMinCostDistanceAlgo(
			//cost.NewBranchAndBoundAlgo(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeDDLInsertion),
			//cost.NewBranchAndBoundAlgo(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeAllPermutation),
			cost.NewBranchAndBoundAlgo(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL),
			cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e20))),
	)
}
