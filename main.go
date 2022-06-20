package main

import (
	"DES-go/metrics"
	"DES-go/schedulers"
	"DES-go/schedulers/allox_scheduler"
	"DES-go/schedulers/hydra_scheduler"
	"DES-go/schedulers/hydra_scheduler/cost"
	"DES-go/schedulers/types"
	"DES-go/simulator"
	"DES-go/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var defaultConfig = &Config{
	CasesPath:   "/hydra/cases",
	ReportsPath: "/hydra/data",
	Workload: "heavy",
	NumberOfJobs: []int{10, 400},
	Algorithms: []string{
		"hydra_alpha_0",
		"hydra_alpha_1",
		"hydra_alpha_3",
		"hydra_alpha_5",
		"hydra_alpha_7",
		"hydra_alpha_9",
		"allox",
		"gavel",
		"chronus",
	},
}

func main() {
	args := os.Args
	var config *Config
	if len(args) == 1 {
		config = defaultConfig
	} else {
		path := args[1]
		log.Printf("specified configuration path: %v", path)
		config = loadConfig(path)
	}
	clusterConfig := make(map[string]int)
	var caseFileName string
	if config.Workload == "light" {
		clusterConfig["V100"] = 15
		clusterConfig["GTX2080Ti"] = 15
		clusterConfig["A100"] = 15
		caseFileName = "20_ddl.csv"
	} else if config.Workload == "heavy" {
		clusterConfig["V100"] = 20
		clusterConfig["GTX2080Ti"] = 15
		clusterConfig["A100"] = 10
		caseFileName = "30_ddl.csv"
	} else {
		panic("configuration param error: Workload must be \"light\" or \"heavy\".")
	}
	if len(config.NumberOfJobs) != 2 {
		panic("configuration param error: NumberOfJobs must be an array of 2 numbers.")
	}
	config.NumberOfJobs[0] = config.NumberOfJobs[0] / 10 * 10
	config.NumberOfJobs[1] = config.NumberOfJobs[1] / 10 * 10
	if config.NumberOfJobs[0] < 0 || config.NumberOfJobs[1] < 0 {
		panic("configuration param error: NumberOfJobs must not be negative.")
	}
	if config.NumberOfJobs[1] > 400 {
		config.NumberOfJobs[1] = 400
	}

	// clusterConfigs 代表集群的配置变化空间，分别传入初始的状态，以及末尾状态，以及增加步长，可以按顺序获取一批gpu配置
	clusterConfigs := []map[string]int{clusterConfig}
	// caseRange 表示，这个case的哪一部分用来做模拟。传入多个caseRange，即做多次实验。
	caseRanges := make([][]int, 0)
	for i := config.NumberOfJobs[0]; i <= config.NumberOfJobs[1]; i += 10 {
		caseRanges = append(caseRanges, []int{0, i})
	}
	log.Printf("config.NumberOfJobs is fixed to [%d, %d]", config.NumberOfJobs[0], config.NumberOfJobs[1])
	algo2type := map[string]SchedulerType {
		"gavel": Gavel,
		"chronus": Chronus,
		"allox": Allox,
		"hydra_alpha_0": HydraPureHeuristic,
		"hydra_alpha_1": HydraBABWithHeuristic1s,
		"hydra_alpha_3": HydraBABWithHeuristic3s,
		"hydra_alpha_5": HydraBABWithHeuristic5s,
		"hydra_alpha_7": HydraBABWithHeuristic7s,
		"hydra_alpha_9": HydraBABWithHeuristic9s,
	}
	schedulerTypes := make([]SchedulerType, 0)
	for _, algo := range config.Algorithms {
		getSt := func(algo string) SchedulerType {
			if t, ok := algo2type[algo]; ok {
				return t
			}
			algos := make([]string, 0, len(algo2type))
			for algo := range algo2type {
				algos = append(algos, algo)
			}
			panic(fmt.Sprintf("configuration param error: algorithm %s not exists! valid algo types: %v", algo, algos))
		}
		schedulerTypes = append(schedulerTypes, getSt(algo))
	}

	reports := doSimulationForMultiClusterConfig(config, caseFileName, clusterConfigs, caseRanges, schedulerTypes)

	metrics.SaveSimulationReport(config.ReportsPath, reports, &metrics.SimulationMetaConfig{
		CaseFileName:   caseFileName,
		CaseRanges:     caseRanges,
		ClusterConfigs: clusterConfigs,
	})
}

func doSimulationForMultiClusterConfig(config *Config, caseFileName string, clusterConfig []map[string]int, caseRanges [][]int, schedulerTypes []SchedulerType) map[string][]*metrics.Report {
	result := make(map[string][]*metrics.Report)
	for _, cc := range clusterConfig {
		m := doSimulationForOneClusterConfig(config, caseFileName, cc, caseRanges, schedulerTypes)
		for k, v := range m {
			if _, ok := result[k]; !ok {
				result[k] = make([]*metrics.Report, 0)
			}
			result[k] = append(result[k], v...)
		}
	}
	return result
}

func doSimulationForOneClusterConfig(config *Config, caseFileName string, clusterConfig map[string]int, caseRanges [][]int, schedulerTypes []SchedulerType) map[string][]*metrics.Report {
	casePath := filepath.Join(config.CasesPath, caseFileName)
	schedulerType2reports := make(map[string][]*metrics.Report)
	fmt.Printf("Starting simulation...\n")
	bs, err := json.Marshal(clusterConfig)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Case Path: %s, Cluster Config: %s\n", casePath, string(bs))
	fmt.Printf("Case Ranges: %+v, SchedulerTypes: %+v\n", casePath, schedulerTypes)
	timeLayout := "2006-01-02_15:04:05"
	startSimulation := time.Now()
	fmt.Printf("Start simulation time: %s\n", startSimulation.Format(timeLayout))
	for _, schedulerType := range schedulerTypes {
		schedulerStart := time.Now()
		fmt.Printf("Starting Simulation For Scheduler %s, StartTime: %s\n", schedulerType, schedulerStart.Format(timeLayout))
		reports := make([]*metrics.Report, 0, len(caseRanges))
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
			reports = append(reports, metrics.GenerateSingleSimulationReport(record))
		}
		schedulerType2reports[string(schedulerType)] = reports
		schedulerEnd := time.Now()
		fmt.Printf("Ending Simulation For Scheduler %s, EndTime: %s, RunTime: %.2f\n",
			schedulerType, schedulerEnd.Format(timeLayout), schedulerEnd.Sub(schedulerStart).Seconds())
	}
	endSimulation := time.Now()
	fmt.Printf("End Simulation Time: %s, RunTime: %.2f\n", endSimulation.Format(timeLayout), endSimulation.Sub(startSimulation).Seconds())
	return schedulerType2reports
}

func getScheduler(schedulerType SchedulerType) types.Scheduler {
	if strings.HasPrefix(string(schedulerType), "HydraBABWithHeuristic") {
		l, _ := strconv.Atoi(strings.Split(string(schedulerType), "_")[1])
		//return initHydraBABHeuristicScheduler(time.Duration(l)*time.Second)
		return initHydraBABHeuristicScheduler(time.Duration(100*l)*time.Millisecond)
	}
	var scheduler types.Scheduler = nil
	switch schedulerType {
	case Dummy:
		scheduler = initDummyScheduler()
	case Gavel:
		scheduler = initSJFScheduler()
	case Chronus:
		scheduler = initEDFScheduler()
	case HydraPureHeuristic:
		scheduler = initHydraHeuristicScheduler()
	case Allox:
		scheduler = initAlloxScheduler()
	default:
		panic("Unsupported scheduler type.")
	}
	return scheduler
}

type SchedulerType string

const (
	Dummy SchedulerType = "Dummy"
	Gavel              SchedulerType = "Gavel"
	Chronus            SchedulerType = "Chronus"
	HydraPureHeuristic SchedulerType = "HydraPureHeuristic"
	HydraBABWithHeuristic1s  SchedulerType = "HydraBABWithHeuristic_1"
	HydraBABWithHeuristic3s  SchedulerType = "HydraBABWithHeuristic_3"
	HydraBABWithHeuristic5s  SchedulerType = "HydraBABWithHeuristic_5"
	HydraBABWithHeuristic7s  SchedulerType = "HydraBABWithHeuristic_7"
	HydraBABWithHeuristic9s  SchedulerType = "HydraBABWithHeuristic_9"
	Allox                    SchedulerType = "Allox"
)

func initDummyScheduler() types.Scheduler {
	return schedulers.NewDummyScheduler()
}

func initSJFScheduler() types.Scheduler {
	return schedulers.NewGavelScheduler()
}

func initEDFScheduler() types.Scheduler {
	return schedulers.NewChronusScheduler()
}

func initAlloxScheduler() types.Scheduler {
	return allox_scheduler.NewAlloxScheduler(false)
}

func initHydraScheduler() types.Scheduler {
	return hydra_scheduler.New(
		hydra_scheduler.WithScheme(hydra_scheduler.NewBasicScheduleScheme(true, false, -1, true)),
		hydra_scheduler.WithDistanceAlgo(hydra_scheduler.NewMinCostDistanceAlgo(
			cost.NewBranchAndBoundAlgo(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL),
			cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e20))),
	)
}

func initHydraHeuristicScheduler() types.Scheduler {
	return hydra_scheduler.New(
		hydra_scheduler.WithScheme(hydra_scheduler.NewBasicScheduleScheme(true, false, -1, true)),
		hydra_scheduler.WithDistanceAlgo(hydra_scheduler.NewMinCostDistanceAlgo(
			cost.NewSwapHeuristic(),
			cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e20))),
	)
}

func initHydraBABHeuristicScheduler(latency time.Duration) types.Scheduler {
	return hydra_scheduler.New(
		hydra_scheduler.WithScheme(hydra_scheduler.NewBasicScheduleScheme(true, false, -1, true)),
		hydra_scheduler.WithDistanceAlgo(hydra_scheduler.NewMinCostDistanceAlgo(
			//cost.NewBranchAndBoundAlgoWithLatency(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL, time.Duration(latencySec)*time.Second, cost.NewSwapHeuristic()),
			cost.NewBranchAndBoundAlgoWithLatency(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL, latency, cost.NewSwapHeuristic()),
			cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e20))),
	)
}

type Config struct {
	CasesPath   string `json:"cases_path"`
	ReportsPath string `json:"reports_path"`
	Workload string `json:"workload"`
	NumberOfJobs []int `json:"number_of_jobs"`
	Algorithms []string `json:"algorithms"`
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
	gpus := []string{"A100", "GTX2080Ti", "V100"}
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
