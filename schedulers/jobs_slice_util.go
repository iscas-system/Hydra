package schedulers

import (
	"DES-go/simulator"
	"DES-go/util"
	"sort"
)

type jobsSliceUtil struct{}

func getJobsSliceUtil() jobsSliceUtil {
	return jobsSliceUtil{}
}

func (u jobsSliceUtil) ReorderToSRTF(gpuType simulator.GPUType, jobs []*simulator.Job) {
	SRTFSorter := util.Sorter{
		LenFunc:  func() int { return len(jobs) },
		LessFunc: func(i, j int) bool { return jobs[i].RemainingDuration(gpuType) < jobs[j].RemainingDuration(gpuType) },
		SwapFunc: func(i, j int) {
			o := jobs[i]
			jobs[i] = jobs[j]
			jobs[j] = o
		},
	}
	sort.Sort(SRTFSorter)
}

func (u jobsSliceUtil) InsertJobsSlice(job *simulator.Job, idx int, jobs []*simulator.Job) []*simulator.Job {
	back := append([]*simulator.Job{}, jobs[idx:]...)
	res := append(jobs[:idx], job)
	res = append(res, back...)
	return res
}
