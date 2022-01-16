package simulator

import (
	"DES-go/schedulers/types"
	"DES-go/util"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type DataSource struct {
	JobMetas              map[types.JobName]*JobMeta // index by job name
	JobNameSortedBySubmit []types.JobName
	CaseRange             []int
}

var dataSourceInstance *DataSource

func getDataSource() *DataSource {
	return dataSourceInstance
}

func initDataSource(csvFilePath string, dataSourceRange []int) {
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

	for _, records := range csvDataRecords {
		for idx, content := range records {
			records[idx] = strings.TrimSpace(content)
		}
	}
	csvHeaders := csvDataRecords[0]
	csvDataRecords = csvDataRecords[1:]

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

	colGPUTypes2Idx := make(map[types.GPUType]int)

	for idx, header := range csvHeaders {
		if len(header) > 0 && util.StringSliceIndexOf([]string{colJobName, colSubmitTime, colDDL}, header) == -1 {
			colGPUTypes2Idx[types.GPUType(header)] = idx
		}
	}

	jobMetas := make(map[types.JobName]*JobMeta)
	jobNamesSortedBySubmitTime := make([]types.JobName, 0, len(csvDataRecords))
	for _, record := range csvDataRecords {
		jobName := types.JobName(record[colJobNameIdx])
		submitTime, err := strconv.ParseFloat(record[colSubmitTimeIdx], 64)
		if err != nil {
			panic(err)
		}
		ddl, err := strconv.ParseFloat(record[colDDLIdx], 64)
		if err != nil {
			panic(err)
		}
		durations := make(map[types.GPUType]types.Duration)
		for gpuType, idx := range colGPUTypes2Idx {
			dur, err := strconv.ParseFloat(record[idx], 64)
			if err != nil {
				panic(err)
			}
			durations[gpuType] = types.Duration(dur)
		}
		jobMetas[jobName] = NewJobMeta(jobName, types.Time(submitTime), types.Time(ddl), durations)
		jobNamesSortedBySubmitTime = append(jobNamesSortedBySubmitTime, jobName)
	}
	dataSourceInstance = &DataSource{
		JobMetas:              jobMetas,
		JobNameSortedBySubmit: jobNamesSortedBySubmitTime,
		CaseRange:             dataSourceRange,
	}
}

func SetDataSource(jobMetas []*JobMeta) {
	metasMap := make(map[types.JobName]*JobMeta)
	for _, meta := range jobMetas {
		metasMap[meta.JobName()] = meta
	}
	jobNames := make([]types.JobName, 0, len(jobMetas))
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

func (ds *DataSource) JobMeta(jobName types.JobName) *JobMeta {
	return ds.JobMetas[jobName]
}

func (ds *DataSource) Duration(jobName types.JobName, gpuType types.GPUType) types.Duration {
	return ds.JobMetas[jobName].durations[gpuType]
}

func (ds *DataSource) SubmitTime(jobName types.JobName) types.Time {
	return ds.JobMetas[jobName].submitTime
}

func (ds *DataSource) DDL(jobName types.JobName) types.Time {
	return ds.JobMetas[jobName].ddl
}

func (ds *DataSource) Durations(jobName types.JobName) map[types.GPUType]types.Duration {
	return ds.JobMetas[jobName].durations
}

func (ds *DataSource) IterBySubmitTime(iterFunc func(indices []int, meta []types.JobMeta)) {
	iterTarget := ds.JobNameSortedBySubmit[ds.CaseRange[0]:ds.CaseRange[1]]
	for i := 0; i < len(iterTarget); i++ {
		metas := make([]types.JobMeta, 0, 1)
		indices := make([]int, 0, 1)
		l := ds.JobMeta(iterTarget[i])
		metas = append(metas, l)
		indices = append(indices, i)
		var j int
		for j = i + 1; j < len(iterTarget); j++ {
			if ds.JobMeta(iterTarget[j]).submitTime == metas[0].SubmitTime() {
				metas = append(metas, ds.JobMeta(iterTarget[j]))
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
