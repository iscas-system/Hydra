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
	//config := loadConfig("/home/yzc/go/src/DES-go/config.json")
	config := loadConfig("/Users/purchaser/go/src/DES-go/config.json")

	// clusterConfigs 代表集群的配置变化空间，分别传入初始的状态，以及末尾状态，以及增加步长，可以按顺序获取一批gpu配置
	clusterConfigs := generateGPUConfig(
		map[string]int{
			"V100": 25,
			"P100": 10,
			"T4":   10,
		}, map[string]int{
			"V100": 25,
			"P100": 10,
			"T4":   10,
		}, 1)

	caseFileName := "case_5000_all_30_ddl.csv"
	// caseRange 表示，这个case的哪一部分用来做模拟。传入多个caseRange，即做多次实验。
	caseRanges := make([][]int, 0)
	for i := 300; i <= 400; i += 10 {
		caseRanges = append(caseRanges, []int{0, i})
	}

	// schedulerTypes 表示了要进行模拟的调度器类型。
	schedulerTypes := []SchedulerType{
		SJF,
		EDF,
		KMeans,
		//Allox,
	}

	// records := doSimulationForOneClusterConfig(config, caseFileName, clusterConfig, caseRanges, schedulerTypes)
	records := doSimulationForMultiClusterConfig(config, caseFileName, clusterConfigs, caseRanges, schedulerTypes)

	metrics.SaveSimulationReport(config.ReportsPath, records, &metrics.SimulationMetaConfig{
		CaseFileName:   caseFileName,
		CaseRanges:     caseRanges,
		ClusterConfigs: clusterConfigs,
	})
}

func doSimulationForMultiClusterConfig(config *Config, caseFileName string, clusterConfig []map[string]int, caseRanges [][]int, schedulerTypes []SchedulerType) map[string][]*types.Record {
	result := make(map[string][]*types.Record)
	for _, cc := range clusterConfig {
		m := doSimulationForOneClusterConfig(config, caseFileName, cc, caseRanges, schedulerTypes)
		for k, v := range m {
			if _, ok := result[k]; !ok {
				result[k] = make([]*types.Record, 0)
			}
			result[k] = append(result[k], v...)
		}
	}
	return result
}

func doSimulationForOneClusterConfig(config *Config, caseFileName string, clusterConfig map[string]int, caseRanges [][]int, schedulerTypes []SchedulerType) map[string][]*types.Record {
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
			record.CaseRange = caseRange
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

func generateGPUConfig(initConfig map[string]int, targetConfig map[string]int, step int) []map[string]int {
	gpus := []string{"T4", "P100", "V100"}
	result := make([]map[string]int, 0)
	result = append(result, initConfig)
	curr := util.CopyStringIntMap(initConfig)
	for {
		keys := util.StringIntMapLessOrEqualsKeys(curr, targetConfig)
		if len(keys) == 0 {
			return result
		}
		util.StringSliceSortBy(keys, gpus)
		for _, key := range keys {
			curr = util.CopyStringIntMap(curr)
			curr[key] += step
			if curr[key] > targetConfig[key] {
				curr[key] = targetConfig[key]
			}
			result = append(result, curr)
		}
	}
}
