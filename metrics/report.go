package metrics

import (
	"DES-go/schedulers/types"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"
	"time"
)

type Report struct {
	CasePath      string         `json:"case_path"`
	SchedulerName string         `json:"scheduler_name"`
	SchedulerInfo interface{}    `json:"scheduler_info"`
	ClusterConfig *ClusterConfig `json:"cluster_config"`
	Execution     *Execution     `json:"execution"`
}

type GPU struct {
	Type string `json:"name"`
}

type ClusterConfig struct {
	GPUs map[string]int `json:"GPUs"`
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
	AverageDDLViolationDurationSeconds float64     `json:"average_ddl_violation_duration_seconds"`
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
	CasePath      string
	ClusterConfig map[types.GPUType]int
}

func SaveSimulationReport(folder string, record *types.Record, config *SimulationMetaConfig) {
	report := generateSimulationReport(record, config)
	fileName := generateFileName(report)
	filePath := path.Join(folder, fileName)
	bs, err := json.MarshalIndent(report, "", "\t")
	if err != nil {
		panic(fmt.Sprintf("Save Report json Marshal failed, err = %s", err.Error()))
	}
	err = ioutil.WriteFile(filePath, bs, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("Save Report WriteFile failed, err = %+v", err))
	}
	fmt.Printf("generate report to %s\n", filePath)
	fmt.Printf("content \n%s\n", string(bs))
}

func generateFileName(report *Report) string {
	caseName := strings.Split(path.Base(report.CasePath), ".")[0]
	datetime := time.Now().Format("01-02_15:04:05")
	return fmt.Sprintf("%s_%s_[jct_%d]_[violated_%d]_%s.json",
		report.SchedulerName,
		caseName,
		int(report.Execution.AverageJCTSeconds),
		report.Execution.DDLViolatedJobsCount,
		datetime)
}

func generateSimulationReport(record *types.Record, config *SimulationMetaConfig) *Report {
	report := &Report{
		CasePath:      config.CasePath,
		SchedulerName: record.Scheduler.Name(),
		SchedulerInfo: record.Scheduler.Info(),
	}
	clusterConfig := &ClusterConfig{GPUs: make(map[string]int)}
	for gpuType, count := range config.ClusterConfig {
		clusterConfig.GPUs[string(gpuType)] = count
	}
	report.ClusterConfig = clusterConfig
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
		AverageDDLViolationDurationSeconds: avgViolatedDuration,
		// DDLViolatedJobs:               packJobs(violatedJobs),
		DDLViolatedJobsCount: len(violatedJobs),
		// FinishedJobs:                  packJobs(record.FinishedJobs),
		FinishedJobsCount:             len(record.FinishedJobs),
		DoScheduleCount:               len(schedulerRecord.DoScheduleRecords),
		AverageDoScheduleDurationMs:   int((sumDoScheduleRecordDuration / time.Duration(len(schedulerRecord.DoScheduleRecords))).Milliseconds()),
		MaxDoScheduleDurationMs:       int(maxDoScheduleRecordDuration.Milliseconds()),
		SchedulerExecutionRecordExtra: schedulerRecord.Extra,
	}
	report.Execution = execution
	return report
}
