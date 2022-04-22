package metrics

import (
	"DES-go/schedulers/types"
	"DES-go/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Reports struct {
	CaseName       string               `json:"case_name"`
	CaseRanges     [][]int              `json:"case_ranges"`
	ClusterConfigs []*ClusterConfig     `json:"cluster_configs"`
	Reports        map[string][]*Report `json:"reports"`
}

type Report struct {
	SchedulerName string         `json:"scheduler_name"`
	SchedulerInfo interface{}    `json:"scheduler_info"`
	ClusterConfig *ClusterConfig `json:"cluster_config"`
	CaseRange     []int          `json:"case_range"`
	Execution     *Execution     `json:"execution"`
}

type GPU struct {
	Type string `json:"name"`
}

type ClusterConfig struct {
	GPUs     map[string]int `json:"GPUs"`
	GPUCount int            `json:"gpu_count"`
}

type Job struct {
	Name               string               `json:"name"`
	SubmitTime         float64              `json:"submit_time"`
	FinishedTime       float64              `json:"finished_time"`
	JCT                float64              `json:"jct"`
	DDL                float64              `json:"ddl"`
	Violated           bool                 `json:"violated"`
	ViolatedDuration   float64              `json:"violated_duration"`
	QueueDelayDuration float64              `json:"queue_delay_duration"`
	ExecutionRanges    []*JobExecutionRange `json:"execution_ranges"`
}

type JobExecutionRange struct {
	GPU       string  `json:"gpu"`
	StartTime float64 `json:"start_time"`
	End       float64 `json:"end"`
	Runtime   float64 `json:"runtime"`
}

type Execution struct {
	AverageJCTSeconds                  float64     `json:"average_jct_seconds"`
	AverageQueueDelaySeconds           float64     `json:"average_queue_delay_seconds"`
	AverageDDLViolationDurationSeconds float64     `json:"average_ddl_violation_duration_seconds"`
	TotalDDLViolationDurationSeconds   float64     `json:"total_ddl_violation_duration_seconds"`
	DDLViolatedJobs                    []*Job      `json:"-"`
	DDLViolatedJobsCount               int         `json:"ddl_violated_jobs_count"`
	FinishedJobs                       []*Job      `json:"-"`
	FinishedJobsCount                  int         `json:"finished_jobs_count"`
	DoScheduleCount                    int         `json:"do_schedule_count"`
	AverageDoScheduleDurationMs        int         `json:"average_do_schedule_duration_ms"`
	MaxDoScheduleDurationMs            int         `json:"max_do_schedule_duration_ms"`
	SchedulerExecutionRecordExtra      interface{} `json:"scheduler_execution_record_extra"`
}

type SimulationMetaConfig struct {
	CaseFileName   string
	CaseRanges     [][]int
	ClusterConfigs []map[string]int
}

func transformClusterConfigs(o []map[string]int) []*ClusterConfig {
	r := make([]*ClusterConfig, 0, len(o))
	for _, c := range o {
		totalCount := 0
		for _, gpuCount := range c {
			totalCount += gpuCount
		}
		r = append(r, &ClusterConfig{
			GPUs:     c,
			GPUCount: totalCount,
		})
	}
	return r
}

func SaveSimulationReport(folder string, schedulerType2Reports map[string][]*Report, config *SimulationMetaConfig) {
	caseName := strings.Split(config.CaseFileName, ".")[0]
	reports := &Reports{
		CaseName:       caseName,
		CaseRanges:     config.CaseRanges,
		ClusterConfigs: transformClusterConfigs(config.ClusterConfigs),
		Reports:        make(map[string][]*Report),
	}
	for schedulerType, rs := range schedulerType2Reports {
		reports.Reports[schedulerType] = rs
	}
	fileName := generateFileName(reports)
	filePath := filepath.Join(folder, fileName)
	bs, err := json.MarshalIndent(reports, "", "\t")
	if err != nil {
		panic(fmt.Sprintf("Save Report json Marshal failed, err = %s", err.Error()))
	}
	err = ioutil.WriteFile(filePath, bs, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("Save Report WriteFile failed, err = %+v", err))
	}
	fmt.Printf("generate report to %s\n", filePath)
	//fmt.Printf("content \n%s\n", string(bs))
}

func generateFileName(reports *Reports) string {
	datetime := time.Now().Format("01-02_15:04:05")
	schedulerNames := make([]string, 0, len(reports.Reports))
	for schedulerName := range reports.Reports {
		schedulerNames = append(schedulerNames, schedulerName[len(schedulerName) - 3:])
	}
	schedulersCombined := util.StringSliceJoinWith(schedulerNames, "_")
	firstCaseRangeCombined := util.IntSliceJoinWith(reports.CaseRanges[0], "_")
	lastCaseRangeCombined := util.IntSliceJoinWith(reports.CaseRanges[len(reports.CaseRanges)-1], "_")
	return fmt.Sprintf("%s_%s_case_range_(%v-%v)_%s.json",
		schedulersCombined,
		reports.CaseName,
		firstCaseRangeCombined,
		lastCaseRangeCombined,
		datetime)
}

func GenerateSingleSimulationReport(record *types.Record) *Report {
	report := &Report{
		SchedulerName: record.SchedulerName,
		SchedulerInfo: record.SchedulerInfo,
	}
	clusterConfig := &ClusterConfig{GPUs: make(map[string]int)}
	for gpuType, gpus := range record.GPUs {
		clusterConfig.GPUs[string(gpuType)] = len(gpus)
		clusterConfig.GPUCount += len(gpus)
	}
	report.ClusterConfig = clusterConfig
	report.CaseRange = record.CaseRange
	schedulerRecord := record.SchedulerRecord
	violatedJobs, avgViolatedDuration := violation(record.FinishedJobs)
	sumDoScheduleRecordDuration := time.Duration(0)
	maxDoScheduleRecordDuration := time.Duration(0)
	for _, doScheduleRecord := range schedulerRecord.DoScheduleRecords {
		sumDoScheduleRecordDuration += doScheduleRecord.Duration
		maxDoScheduleRecordDuration = time.Duration(math.Max(float64(doScheduleRecord.Duration), float64(maxDoScheduleRecordDuration)))
	}
	execution := &Execution{
		AverageJCTSeconds:                  avgJCT(record.FinishedJobs),
		AverageQueueDelaySeconds:           avgQueuingDelay(record.FinishedJobs),
		AverageDDLViolationDurationSeconds: avgViolatedDuration,
		TotalDDLViolationDurationSeconds:   avgViolatedDuration * float64(len(violatedJobs)),
		// DDLViolatedJobs:               packJobs(violatedJobs),
		DDLViolatedJobsCount:             len(violatedJobs),
		// FinishedJobs:                  packJobs(record.FinishedJobs),
		FinishedJobsCount:             len(record.FinishedJobs),
		DoScheduleCount:               len(schedulerRecord.DoScheduleRecords),
		//AverageDoScheduleDurationMs:   int((sumDoScheduleRecordDuration / time.Duration(len(schedulerRecord.DoScheduleRecords))).Milliseconds()),
		MaxDoScheduleDurationMs:       int(maxDoScheduleRecordDuration.Milliseconds()),
		SchedulerExecutionRecordExtra: schedulerRecord.Extra,
	}
	report.Execution = execution
	return report
}
