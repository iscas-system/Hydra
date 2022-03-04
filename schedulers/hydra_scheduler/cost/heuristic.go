package cost

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/schedulers/types"
	"DES-go/util"
	"sort"
)

type MinCostAlgoByHeuristic interface {
	MinCostAlgo
}

type SwapHeuristic struct {
	LeftThreshold int
}

func NewSwapHeuristic() *SwapHeuristic {
	return &SwapHeuristic{}
}

func NewSwapHeuristicWithLeftThreshold(leftThreshold int) *SwapHeuristic {
	return &SwapHeuristic{LeftThreshold: leftThreshold}
}

type SwapHeuristicMinCostParams struct {
	*MinCostParams
	NeedReorderToSRTF bool
}

func (s SwapHeuristic) String() string {
	return "SwapHeuristic"
}

func (s SwapHeuristic) RecordExtra() interface{} {
	return nil
}


type SwapHeuristicShiftingList struct {
	I int // 移动列表的最左侧任务index。闭区间。
	J int // 移动列表的最右侧任务index。开区间。
}

func (s *SwapHeuristic) MinCost(params *MinCostParams) (float64, []types.Job) {
	return s.minCost(&SwapHeuristicMinCostParams{
		MinCostParams:     params,
		NeedReorderToSRTF: true,
	})
}

// MinCost SwapHeuristic 使用基于交换的启发式算法。
// 具体方法：先将任务按照SJF排序。从右向左遍历，直到找到有ddl违约的任务。
// 找到首个ddl违约的任务后，将它加入一个移动列表，表示我们要将这个移动列表内的任务向左移动。
// 移动列表内的任务发生变化时，将移动列表内的任务进行重新排序，排序的标准为：将任务
// 按照它有多么容易违约ddl排序。越容易违约的任务放在越靠前。
// 使用JCTOffset作为起始时间，任务的ddl时刻减去任务在该GPU上的runtime，该值可作为排序标准。
// 该值越小，则表示越容易突破DDL。
// 将移动列表向左移动，当移动后：
// 1. 移动到右侧的那个任务产生ddl违约，则将它加入移动列表。每当有新任务加入移动列表后，将它们排序，
//    一旦发现最右侧有任务的ddl已经得到满足，则将它从移动列表去除。
// 2. 移动到右侧的任务没有产生ddl违约，而且移动列表最右侧的任务依然违约，则继续移动。
// 3. 移动到右侧的任务没有产生ddl违约，但移动列表最右侧出现了满足ddl约束的任务，则将它们从移动列表去除。
func (s *SwapHeuristic) minCost(params *SwapHeuristicMinCostParams) (float64, []types.Job) {
	jobsUtil := jobs_util.GetJobsSliceUtil()
	if params.NeedReorderToSRTF {
		jobsUtil.ReorderToSRTF(params.GPU.Type(), params.Jobs)
		costResp := params.CostSolver.Cost(params.GPU, params.Jobs)
		if !costResp.DDLViolated {
			return costResp.Cost, params.Jobs
		}
	}
	// 初始化ShiftingList
	shiftingList := &SwapHeuristicShiftingList{}
	s.InitShiftingList(shiftingList, params)
	// 启动算法
	for {
		// 每个循环的开始，先将没有必要在ShiftList中的任务剔除。
		s.RemoveUnnecessaryJobsInShiftingList(shiftingList, params)
		if shiftingList.I == shiftingList.J {
			s.InitShiftingList(shiftingList, params)
		}
		// 检查结束后，向左移动ShiftingList。
		if !s.ShiftLeft(shiftingList, params.Jobs) {
			// 移动失败，则说明已经到头，直接退出。
			break
		}
		// 检查移动到右侧的任务是否产生ddl违约。
		jobsViolatedChecker := s.JobDDLViolatedChecker(params)
		if jobsViolatedChecker(params.Jobs[shiftingList.J]) {
			// 将该右侧的任务加入到移动列表中来。
			shiftingList.J++
			continue
		}
	}
	costResp := params.CostSolver.Cost(params.GPU, params.Jobs)
	return costResp.Cost, params.Jobs
}

func (s *SwapHeuristic) RemoveUnnecessaryJobsInShiftingList(shiftingList *SwapHeuristicShiftingList, params *SwapHeuristicMinCostParams) {
	checker := s.JobDDLViolatedChecker(params)
	for shiftingList.J > shiftingList.I && !checker(params.Jobs[shiftingList.J - 1]) {
		shiftingList.J--
	}
}

func (s *SwapHeuristic) InitShiftingList(shiftingList *SwapHeuristicShiftingList, params *SwapHeuristicMinCostParams) {
	checker := s.JobDDLViolatedChecker(params)
	for i := len(params.Jobs) - 1; i >= 0; i-- {
		if checker(params.Jobs[i]) {
			shiftingList.I = i
			shiftingList.J = i + 1
			return
		}
	}
	shiftingList.I = 0
	shiftingList.J = 0
}

func (s *SwapHeuristic) JobDDLViolatedChecker(params *SwapHeuristicMinCostParams) func(job types.Job) bool {
	costResp := params.CostSolver.Cost(params.GPU, params.Jobs)
	// 1. 首先检查移动到右侧的任务是否产生ddl违约。
	violatedJobMap := jobs_util.GetJobsSliceUtil().Slice2Map(costResp.DDLViolatedJobs)
	jobViolated := func(job types.Job) bool {
		_, ok := violatedJobMap[job.JobName()]
		return ok
	}
	return jobViolated
}

func (s *SwapHeuristic) ReorderJobsByViolationProb(jobs []types.Job, GPUType types.GPUType) {
	violationProb := func(job types.Job) float64 {
		return float64(job.JobMeta().DDL()) - float64(job.RemainingDuration(GPUType))
	}
	sorter := &util.Sorter{
		LenFunc: func() int {
			return len(jobs)
		},
		LessFunc: func(i, j int) bool {
			return violationProb(jobs[i]) < violationProb(jobs[j])
		},
		SwapFunc: func(i, j int) {
			o := jobs[i]
			jobs[j] = jobs[i]
			jobs[i] = o
		},
	}
	sort.Sort(sorter)
}

func (s *SwapHeuristic) ShiftLeft(sl *SwapHeuristicShiftingList, jobs []types.Job) bool {
	if sl.I <= s.LeftThreshold {
		return false
	}
	victim := jobs[sl.I - 1]
	copy(jobs[sl.I - 1: sl.J - 1], jobs[sl.I: sl.J])
	jobs[sl.J - 1] = victim
	sl.I--
	sl.J--
	return true
}