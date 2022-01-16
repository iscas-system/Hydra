package main

import (
	"DES-go/metrics"
	"DES-go/schedulers"
	"DES-go/schedulers/allox_scheduler"
	"DES-go/schedulers/kmeans_scheduler"
	"DES-go/schedulers/kmeans_scheduler/cost"
	"DES-go/schedulers/types"
	"DES-go/simulator"
	"DES-go/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"time"
)

func main() {
	config := loadConfig("/Users/purchaser/go/src/DES-go/config.json")

	clusterConfig := map[types.GPUType]int{
		"V100": 5,
		"P100": 10,
		"T4":   15,
	}

	//caseFileName := "case1.csv"
	//caseRanges := []int{{0, 5}}

	//caseFileName := "case_50_all.csv"
	//caseRanges := [][]int{{0, 30}, {0, 40}, {0, 50}}

	//caseFileName := "case_200_all.csv"
	//caseFileName := "case_200_all_2.csv"
	//caseFileName := "case_200_all_3.csv"
	//caseRanges := make([][]int, 0)
	//for i := 10; i <= 200; i += 10 {
	//	caseRanges = append(caseRanges, []int{0, i})
	//}

	caseFileName := "case_500_all.csv"
	//casePath := "case_500_long_all.csv"
	caseRanges := make([][]int, 0)
	for i := 10; i <= 300; i += 10 {
		caseRanges = append(caseRanges, []int{0, i})
	}
	// caseRange 表示，这个case的哪一部分用来做模拟。传入多个caseRange，即做多次实验。

	// schedulerTypes 表示了要进行模拟的调度器类型。
	schedulerTypes := []SchedulerType{
		SJF,
		EDF,
		KMeans,
		Allox,
	}

	records := doSimulation(config, caseFileName, clusterConfig, caseRanges, schedulerTypes)

	metrics.SaveSimulationReport(config.ReportsPath, records, &metrics.SimulationMetaConfig{
		CaseFileName:  caseFileName,
		CaseRanges:    caseRanges,
		ClusterConfig: clusterConfig,
	})
}

func doSimulation(config *Config, caseFileName string, clusterConfig map[types.GPUType]int, caseRanges [][]int, schedulerTypes []SchedulerType) map[string][]*types.Record {
	casePath := path.Join(config.CasesPath, caseFileName)
	schedulerType2records := make(map[string][]*types.Record)
	fmt.Printf("Starting simulation...\n")
	fmt.Printf("Case Path: %s, Cluster Config: %+v\n", casePath, util.Pretty(clusterConfig))
	fmt.Printf("Case Ranges: %+v, SchedulerTypes: %+v\n", casePath, util.Pretty(clusterConfig))
	timeLayout := "2006-01-02_15:04:05"
	startSimulation := time.Now()
	fmt.Printf("Start simulation time: %s\n", startSimulation.Format(timeLayout))
	for _, schedulerType := range schedulerTypes {
		schedulerStart := time.Now()
		fmt.Printf("Starting Simulation For Scheduler %s, StartTime: %s\n", schedulerType, schedulerStart.Format(timeLayout))
		recordsSlice := make([]*types.Record, 0, len(caseRanges))
		for _, caseRange := range caseRanges {
			start := time.Now()
			fmt.Printf("Simulation For Scheduler %s, CaseRange: %d, StartTime: %s\n",
				schedulerType, caseRange, start.Format(timeLayout))
			scheduler := getScheduler(schedulerType)
			simu := simulator.NewSimulator(scheduler,
				simulator.WithOptionDataSourceRange(caseRange[0], caseRange[1]),
				simulator.WithOptionLogPrintLevel(simulator.NoPrint),
				simulator.WithOptionDataSourceCSVPath(casePath),
				simulator.WithOptionGPUType2Count(clusterConfig))
			record := simu.Run()
			end := time.Now()
			duration := end.Sub(start)
			fmt.Printf("Simulation For Scheduler %s, CaseRange: %d Finished, EndTime: %s, RunTime: %.2f\n",
				schedulerType, caseRange, end.Format(timeLayout), duration.Seconds())
			recordsSlice = append(recordsSlice, record)
		}
		schedulerType2records[string(schedulerType)] = recordsSlice
		schedulerEnd := time.Now()
		fmt.Printf("Ending Simulation For Scheduler %s, EndTime: %s, RunTime: %.2f\n",
			schedulerType, schedulerEnd.Format(timeLayout), schedulerEnd.Sub(schedulerStart).Seconds())
	}
	endSimulation := time.Now()
	fmt.Printf("End Simulation Time: %s, RunTime: %.2f\n", endSimulation.Format(timeLayout), endSimulation.Sub(startSimulation).Seconds())
	return schedulerType2records
}

func getScheduler(schedulerType SchedulerType) types.Scheduler {
	var scheduler types.Scheduler = nil
	switch schedulerType {
	case Dummy:
		scheduler = initDummyScheduler()
	case SJF:
		scheduler = initSJFScheduler()
	case EDF:
		scheduler = initEDFScheduler()
	case KMeans:
		scheduler = initKMeansScheduler()
	case Allox:
		scheduler = initAlloxScheduler()
	default:
		panic("Unsupported scheduler type.")
	}
	return scheduler
}

type SchedulerType string

const (
	Dummy  SchedulerType = "Dummy"
	SJF    SchedulerType = "SJF"
	EDF    SchedulerType = "EDF"
	KMeans SchedulerType = "KMeans"
	Allox  SchedulerType = "Allox"
)

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

type Config struct {
	CasesPath   string `json:"cases_path"`
	ReportsPath string `json:"reports_path"`
}

func loadConfig(configPath string) *Config {
	bytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
	config := &Config{}
	err = json.Unmarshal(bytes, config)
	if err != nil {
		panic(err)
	}
	return config
}
