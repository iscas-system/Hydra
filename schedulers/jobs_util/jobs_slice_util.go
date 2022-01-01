package jobs_util

import (
	"DES-go/schedulers/types"
	"DES-go/util"
	"sort"
)

type JobsSliceUtil struct{}

func GetJobsSliceUtil() JobsSliceUtil {
	return JobsSliceUtil{}
}

func (u JobsSliceUtil) ReorderToSRTF(gpuType types.GPUType, jobs []types.Job) {
	SRTFSorter := util.Sorter{
		LenFunc:  func() int { return len(jobs) },
		LessFunc: func(i, j int) bool { return jobs[i].RemainingDuration(gpuType) < jobs[j].RemainingDuration(gpuType) },
		SwapFunc: func(i, j int) {
			o := jobs[i]
			jobs[i] = jobs[j]
			jobs[j] = o
		},
	}
	if sort.IsSorted(SRTFSorter) {
		return
	}
	sort.Sort(SRTFSorter)
}

func (u JobsSliceUtil) Intersects(jobs1 []types.Job, jobs2 []types.Job) []types.Job {
	less := jobs1
	more := jobs2
	if len(jobs2) < len(jobs1) {
		less = jobs2
		more = jobs1
	}
	intersects := make([]types.Job, 0)
	moreMap := make(map[types.JobName]types.Job)
	for _, j := range more {
		moreMap[j.JobName()] = j
	}
	for _, j := range less {
		if _, ok := moreMap[j.JobName()]; ok {
			intersects = append(intersects, j)
		}
	}
	return intersects
}

func (u JobsSliceUtil) ReorderByJobName(jobs []types.Job) {
	jobNameSorter := util.Sorter{
		LenFunc:  func() int { return len(jobs) },
		LessFunc: func(i, j int) bool { return jobs[i].JobName() < jobs[j].JobName() },
		SwapFunc: func(i, j int) {
			o := jobs[i]
			jobs[i] = jobs[j]
			jobs[j] = o
		},
	}
	if sort.IsSorted(jobNameSorter) {
		return
	}
	sort.Sort(jobNameSorter)
}

func (u JobsSliceUtil) InsertJobsSlice(job types.Job, idx int, jobs []types.Job) []types.Job {
	back := append([]types.Job{}, jobs[idx:]...)
	res := append(jobs[:idx], job)
	res = append(res, back...)
	return res
}

func (u JobsSliceUtil) RemoveJobsSlice(idx int, jobs []types.Job) (types.Job, []types.Job) {
	removed := jobs[idx]
	jobs = append(jobs[:idx], jobs[idx+1:]...)
	return removed, jobs
}

func (u JobsSliceUtil) Copy(jobs []types.Job) []types.Job {
	c := make([]types.Job, len(jobs))
	copy(c, jobs)
	return c
}
