package simulator

import (
	"DES-go/util"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
)

type DataSource struct {
	JobMetas              map[JobName]*JobMeta // index by job name
	JobNameSortedBySubmit []JobName
}

var dataSourceInstance *DataSource

func getDataSource() *DataSource {
	return dataSourceInstance
}

func initDataSource(csvFilePath string) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fmt.Printf("DataSource reading csv from %s...\n", csvFilePath)

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 0
	csvDataRecords, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	fmt.Printf("DataSource reading %d lines of records from %s...\n", len(csvDataRecords), csvFilePath)

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
	dataSourceInstance = &DataSource{
		JobMetas:              jobMetas,
		JobNameSortedBySubmit: jobNamesSortedBySubmitTime,
	}
}

func SetDataSource(jobMetas []*JobMeta, ) {
	metasMap := make(map[JobName]*JobMeta)
	for _, meta := range jobMetas {
		metasMap[meta.JobName()] = meta
	}
	jobNames := make([]JobName, 0, len(jobMetas))
	for _, meta := range jobMetas {
		jobNames = append(jobNames, meta.JobName())
	}

	sorter := util.Sorter{
		LenFunc: func() int {
			return len(jobNames)
		},
		LessFunc: func(i, j int) bool {
			return metasMap[jobNames[i]].SubmitTime() < metasMap[jobNames[j]].SubmitTime()
		},
		SwapFunc: func(i, j int) {
			o := jobNames[i]
			jobNames[i] = jobNames[j]
			jobNames[j] = o
		},
	}
	sort.Sort(sorter)
	ds := &DataSource{
		JobMetas:              metasMap,
		JobNameSortedBySubmit: jobNames,
	}
	dataSourceInstance = ds
}

func (ds *DataSource) JobMeta(jobName JobName) *JobMeta {
	return ds.JobMetas[jobName]
}

func (ds *DataSource) Duration(jobName JobName, gpuType GPUType) Duration {
	return ds.JobMetas[jobName].durations[gpuType]
}

func (ds *DataSource) SubmitTime(jobName JobName) Time {
	return ds.JobMetas[jobName].submitTime
}

func (ds *DataSource) DDL(jobName JobName) Time {
	return ds.JobMetas[jobName].ddl
}

func (ds *DataSource) Durations(jobName JobName) map[GPUType]Duration {
	return ds.JobMetas[jobName].durations
}

func (ds *DataSource) IterBySubmitTime(iterFunc func(indices []int, meta []*JobMeta)) {
	for i := 0; i < len(ds.JobNameSortedBySubmit); i++ {
		metas := make([]*JobMeta, 0, 1)
		indices := make([]int, 0, 1)
		l := ds.JobMeta(ds.JobNameSortedBySubmit[i])
		metas = append(metas, l)
		indices = append(indices, i)
		var j int
		for j = i + 1; j < len(ds.JobNameSortedBySubmit); j++ {
			if ds.JobMeta(ds.JobNameSortedBySubmit[j]).submitTime == metas[0].submitTime {
				metas = append(metas, ds.JobMeta(ds.JobNameSortedBySubmit[j]))
				indices = append(indices, j)
			} else {
				j--
				break
			}
		}
		i = j
		iterFunc(indices, metas)
	}
}
