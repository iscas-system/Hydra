package simulator

import (
	"DES-go/util"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
)

type DataSource struct {
	jobMetas              map[JobName]*JobMeta // index by job name
	jobNameSortedBySubmit []JobName
	gpuTypes              []GPUType
}

var dataSourceInstance *DataSource

func GetDataSource() *DataSource {
	return dataSourceInstance
}

func InitDataSource(csvFilePath string) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 0
	csvDataRecords, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	for _, record := range csvDataRecords {
		log.Printf("read record [%+v]", record)
	}
	csvHeaders := csvDataRecords[0]
	colIndexOf := func(colName string) int {
		res := util.StringSliceIndexOf(csvHeaders, colName)
		if res == -1 {
			panic(fmt.Sprintf("%s not in csvHeaders %+v", colName, csvHeaders))
		}
		return res
	}
	colJobName := "job_name"
	colSubmitTime := "norm_job_submit_time"
	colDDL := "ddl"

	colJobNameIdx := colIndexOf(colJobName)
	colSubmitTimeIdx := colIndexOf(colSubmitTime)
	colDDLIdx := colIndexOf(colDDL)

	colGPUTypes2Idx := make(map[GPUType]int)

	for idx, header := range csvHeaders {
		if len(header) > 0 && util.StringSliceIndexOf([]string{colJobName, colSubmitTime, colDDL}, header) == -1 {
			colGPUTypes2Idx[GPUType(header)] = idx
		}
	}

	jobMetas := make(map[JobName]*JobMeta)
	jobNamesSortedBySubmitTime := make([]JobName, 0, len(csvDataRecords)-1)
	for _, record := range csvDataRecords[1:] {
		jobName := JobName(record[colJobNameIdx])
		submitTime, err := strconv.ParseFloat(record[colSubmitTimeIdx], 64)
		if err != nil {
			panic(err)
		}
		ddl, err := strconv.ParseFloat(record[colDDLIdx], 64)
		if err != nil {
			panic(err)
		}
		durations := make(map[GPUType]Duration)
		for gpuType, idx := range colGPUTypes2Idx {
			dur, err := strconv.ParseFloat(record[idx], 64)
			if err != nil {
				panic(err)
			}
			durations[gpuType] = Duration(dur)
		}
		jobMetas[jobName] = NewJobMeta(jobName, Time(submitTime), Time(ddl), durations)
		jobNamesSortedBySubmitTime = append(jobNamesSortedBySubmitTime, jobName)
	}
	gpuTypes := func() []GPUType {
		res := make([]GPUType, 0, len(colGPUTypes2Idx))
		for gpuType := range colGPUTypes2Idx {
			res = append(res, gpuType)
		}
		return res
	}
	dataSourceInstance = &DataSource{
		jobMetas:              jobMetas,
		jobNameSortedBySubmit: jobNamesSortedBySubmitTime,
		gpuTypes:              gpuTypes(),
	}
}

func (ds *DataSource) JobMeta(jobName JobName) *JobMeta {
	return ds.jobMetas[jobName]
}

func (ds *DataSource) Duration(jobName JobName, gpuType GPUType) Duration {
	return ds.jobMetas[jobName].Durations()[gpuType]
}

func (ds *DataSource) SubmitTime(jobName JobName) Time {
	return ds.jobMetas[jobName].SubmitTime()
}

func (ds *DataSource) DDL(jobName JobName) Time {
	return ds.jobMetas[jobName].DDL()
}

func (ds *DataSource) Durations(jobName JobName) map[GPUType]Duration {
	return ds.jobMetas[jobName].Durations()
}

func (ds *DataSource) IterBySubmitTime(iterFunc func(indices []int, meta []*JobMeta)) {
	for i := 0; i < len(ds.jobNameSortedBySubmit); i++ {
		metas := make([]*JobMeta, 0, 1)
		indices := make([]int, 0, 1)
		l := ds.JobMeta(ds.jobNameSortedBySubmit[i])
		metas = append(metas, l)
		indices = append(indices, i)
		var j int
		for j = i + 1; j < len(ds.jobNameSortedBySubmit); j++ {
			if ds.JobMeta(ds.jobNameSortedBySubmit[j]).SubmitTime() == metas[0].SubmitTime() {
				metas = append(metas, ds.JobMeta(ds.jobNameSortedBySubmit[j]))
				indices = append(indices, j)
			} else {
				break
			}
		}
		i = j
		iterFunc(indices, metas)
	}
}
