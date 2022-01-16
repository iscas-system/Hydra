package cost

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/schedulers/types"
	"DES-go/util"
	"container/heap"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

type MinCostAlgoByBranchAndBound interface {
	MinCostAlgo

	// initNodes
	// 初始化分支限界的节点。
	initNodes(params *MinCostParams) []*BranchAndBoundNode
	// expandNode
	// 当遇到一个未在当前节点内的新节点时，扩展它，返回多个任务序列。
	expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *MinCostParams) [][]types.Job
	// predict
	// 对当前的扩展节点计算一个预测的cost，将其他未在队列的jobs都加入进来，并按照SJF排序，计算一个cost。它必定>=最优解的cost。
	// 如果预测的cost小于minCost，则将minCost更新为predictCost，这样能够任意一个答案节点计算出来之前，获得一个cost上界。
	// 这个predictValidCost是一个可行解，但不一定是最优解，所以为了在扩展节点时高效剪枝，还需要计算一个成本下界cHat，用来做限界函数。
	// cHat为当前这个分支上，成本的下界，即这个序列的成本不可能 <= cHat。如果cHat都比minCost大，那么这条分支就不可能取最优解。
	predict(newJobs []types.Job,
		otherJobs []types.Job,
		params *MinCostParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job)

	String() string
}

type BranchAndBoundNode struct {
	jobs []types.Job
	// cost 这个节点存储的部分jobs的成本。它是不完全的，必定小于将完整的job都加入后的cost。
	cost float64
	// predictCost 如果将不在这个节点的jobs都加入进来，并按照SJF进行排序，给出一个预测的Cost。
	// 它可以用于快速判断minCost的下界，使得当没有最终答案节点算出时，也能修正minCost的值，达到快速剪枝的目的。
	predictCost float64
	// cHat 如果将不在这个节点的jobs都加入进来，那么我们能计算出一个cost的下界，保证我们的解不可能比它更好。
	// 那么在展开节点时，可以依据它来剪枝：若cHat都比当前的最优解minCost要差，那么当前分支不可能产生比minCost更优的解。
	cHat float64
}

type BranchAndBoundLCStandard string
type BranchAndBoundAlgoType int

const (
	BranchAndBoundLCStandardPartialCost BranchAndBoundLCStandard = "PartialCost"
	BranchAndBoundLCStandardPredictCost BranchAndBoundLCStandard = "PredictCost"

	BranchAndBoundAlgoTypeAllPermutation BranchAndBoundAlgoType = 0
	BranchAndBoundAlgoTypeDDLInsertion   BranchAndBoundAlgoType = 1
	BranchAndBoundAlgoTypeFixNonDDL      BranchAndBoundAlgoType = 2
)

type BranchAndBoundSummaryRecord struct {
	CallRecords                         map[int][]*BranchAndBoundCallRecord `json:"-"` // jobsCount to record
	AverageDurationNano                 int                                 `json:"average_duration_nano"`
	AverageExpandNodesCount             float64                             `json:"average_expand_nodes_count"`
	AverageJobsCount                    float64                             `json:"average_jobs_count"`
	MaxJobsCount                        int                                 `json:"max_jobs_count"`
	AverageTotalCutCount                float64                             `json:"average_total_cut_count"`
	AverageAfterExpandCutCount          float64                             `json:"average_after_expand_cut_count"`
	AveragePredictionIsOptimusCutCount  float64                             `json:"average_prediction_is_optimus_cut_count"`
	AveragePredictionReduceMinCostCount float64                             `json:"average_prediction_reduce_min_cost_count"`
	AverageExpandNothingCutCount        float64                             `json:"average_expand_nothing_cut_count"`
	AverageCHatCutCount                 float64                             `json:"average_c_hat_cut_count"`
	JobsCount2SummaryRecord             []interface{}                       `json:"jobs_count_2_summary_record"`
}

type BranchAndBoundCallRecord struct {
	Duration                     time.Duration `json:"duration"`
	JobsCount                    int           `json:"jobs_count"`
	ExpandNodesCount             int           `json:"expand_nodes_count"`
	TotalCutCount                int           `json:"total_cut_count"`
	ExpandNothingCutCount        int           `json:"expand_nothing_cut_count"`
	CHatCutCount                 int           `json:"c_hat_cut_count"`
	AfterExpandCutCount          int           `json:"after_expand_cut_count"`
	PredictionIsOptimusCutCount  int           `json:"prediction_is_optimus_cut_count"`
	PredictionReduceMinCostCount int           `json:"prediction_reduce_min_cost_count"`
}

type BranchAndBoundTemplate struct {
	LCStandard BranchAndBoundLCStandard
	impl       MinCostAlgoByBranchAndBound

	Record   *BranchAndBoundSummaryRecord
	RecordMu *sync.Mutex
}

func NewBranchAndBoundAlgo(LCStandard BranchAndBoundLCStandard, algoType BranchAndBoundAlgoType) MinCostAlgoByBranchAndBound {
	template := &BranchAndBoundTemplate{
		LCStandard: LCStandard,
		Record: &BranchAndBoundSummaryRecord{
			CallRecords: make(map[int][]*BranchAndBoundCallRecord),
		},
		RecordMu: &sync.Mutex{},
	}
	impl := func() MinCostAlgoByBranchAndBound {
		switch algoType {
		case BranchAndBoundAlgoTypeAllPermutation:
			return &BranchAndBoundAllPermutation{template}
		case BranchAndBoundAlgoTypeDDLInsertion:
			return &BranchAndBoundDDLInsertion{template}
		case BranchAndBoundAlgoTypeFixNonDDL:
			return &BranchAndBoundFixNonDDL{&BranchAndBoundAllPermutation{template}}
		default:
			panic("Unsupported algoType")
		}
	}()
	template.impl = impl
	return impl
}

func (m *BranchAndBoundTemplate) initNodes(params *MinCostParams) []*BranchAndBoundNode {
	panic("Template method should not be called.")
}

func (m *BranchAndBoundTemplate) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *MinCostParams) [][]types.Job {
	panic("Template method should not be called.")
}

func (m *BranchAndBoundTemplate) predict(
	jobNamesInCurrNode map[types.JobName]bool,
	newJob types.Job,
	newJobs []types.Job,
	params *MinCostParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job) {
	panic("Template method should not be called.")
}

func (m *BranchAndBoundTemplate) String() string {
	return fmt.Sprintf("BranchAndBoundTemplate[impl=%s]", m.impl)
}

func (m *BranchAndBoundTemplate) RecordExtra() interface{} {
	recordsCount := 0
	sumAllJobsCount := 0
	sumAllTotalCutCount := 0
	sumAllCHatCutCount := 0
	sumAllDuration := time.Duration(0)
	sumAllExpandNodesCount := 0
	sumAllAfterExpandCutCount := 0
	sumAllPredictionIsOptimusCutCount := 0
	sumAllPredictionReduceMinCostCount := 0
	sumAllExpandNothingCutCount := 0
	maxJobsCount := 0
	type specificJobsCountSummaryRecord struct {
		CallCount                           int     `json:"call_count"`
		JobsCount                           int     `json:"jobs_count"`
		AverageExpandNodesCount             float64 `json:"average_expand_nodes_count"`
		AverageDurationNano                 int     `json:"average_duration_nano"`
		AverageTotalCutCount                float64 `json:"average_total_cut_count"`
		AverageExpandNothingCutCount        float64 `json:"average_expand_nothing_cut_count"`
		AverageCHatCutCount                 float64 `json:"average_c_hat_cut_count"`
		AverageAfterExpandCutCount          float64 `json:"average_after_expand_cut_count"`
		AveragePredictionIsOptimusCutCount  float64 `json:"average_prediction_is_optimus_cut_count"`
		AveragePredictionReduceMinCostCount float64 `json:"average_prediction_reduce_min_cost_count"`
	}
	JobsCount2SummaryRecord := make([]interface{}, 0)
	for jobsCount, records := range m.Record.CallRecords {
		currJobsCountRecordsCount := len(records)
		sumAllJobsCount += currJobsCountRecordsCount * jobsCount
		recordsCount += currJobsCountRecordsCount
		if jobsCount > maxJobsCount {
			maxJobsCount = jobsCount
		}
		sumDuration := time.Duration(0)
		sumExpandNodesCount := 0
		sumTotalCutCount := 0
		sumExpandNothingCutCount := 0
		sumCHatCutCount := 0
		sumAfterExpandCutCount := 0
		sumPredictionIsOptimusCutCount := 0
		sumPredictionReduceMinCostCount := 0
		for _, record := range records {
			// 执行开销统计。
			sumAllDuration += record.Duration
			sumDuration += record.Duration

			// 扩展节点的数量。
			sumAllExpandNodesCount += record.ExpandNodesCount
			sumExpandNodesCount += record.ExpandNodesCount

			// 剪枝信息记录
			sumAllTotalCutCount += record.TotalCutCount
			sumTotalCutCount += record.TotalCutCount

			sumAllExpandNothingCutCount += record.ExpandNothingCutCount
			sumExpandNothingCutCount += record.ExpandNothingCutCount

			sumAllCHatCutCount += record.CHatCutCount
			sumCHatCutCount += record.CHatCutCount

			sumAllAfterExpandCutCount += record.AfterExpandCutCount
			sumAfterExpandCutCount += record.AfterExpandCutCount

			sumAllPredictionIsOptimusCutCount += record.PredictionIsOptimusCutCount
			sumPredictionIsOptimusCutCount += record.PredictionIsOptimusCutCount

			// 统计prediction有效减小minCost的次数。
			sumAllPredictionReduceMinCostCount += record.PredictionReduceMinCostCount
			sumPredictionReduceMinCostCount += record.PredictionReduceMinCostCount
		}
		JobsCount2SummaryRecord = append(JobsCount2SummaryRecord, &specificJobsCountSummaryRecord{
			JobsCount:                           jobsCount,
			CallCount:                           currJobsCountRecordsCount,
			AverageExpandNodesCount:             float64(sumExpandNodesCount) / float64(currJobsCountRecordsCount),
			AverageDurationNano:                 int((sumDuration / time.Duration(currJobsCountRecordsCount)).Nanoseconds()),
			AverageTotalCutCount:                float64(sumTotalCutCount) / float64(currJobsCountRecordsCount),
			AverageExpandNothingCutCount:        float64(sumExpandNothingCutCount) / float64(currJobsCountRecordsCount),
			AverageCHatCutCount:                 float64(sumCHatCutCount) / float64(currJobsCountRecordsCount),
			AverageAfterExpandCutCount:          float64(sumAfterExpandCutCount) / float64(currJobsCountRecordsCount),
			AveragePredictionIsOptimusCutCount:  float64(sumPredictionIsOptimusCutCount) / float64(currJobsCountRecordsCount),
			AveragePredictionReduceMinCostCount: float64(sumPredictionReduceMinCostCount) / float64(currJobsCountRecordsCount),
		})
	}
	m.Record.MaxJobsCount = maxJobsCount
	m.Record.AverageExpandNodesCount = float64(sumAllExpandNodesCount) / float64(recordsCount)
	m.Record.AverageJobsCount = float64(sumAllJobsCount) / float64(recordsCount)
	m.Record.AverageDurationNano = int((sumAllDuration / time.Duration(recordsCount)).Nanoseconds())
	m.Record.AverageTotalCutCount = float64(sumAllTotalCutCount) / float64(recordsCount)
	m.Record.AverageExpandNothingCutCount = float64(sumAllExpandNothingCutCount) / float64(recordsCount)
	m.Record.AverageCHatCutCount = float64(sumAllCHatCutCount) / float64(recordsCount)
	m.Record.AverageAfterExpandCutCount = float64(sumAllAfterExpandCutCount) / float64(recordsCount)
	m.Record.AveragePredictionIsOptimusCutCount = float64(sumAllPredictionIsOptimusCutCount) / float64(recordsCount)
	m.Record.AveragePredictionReduceMinCostCount = float64(sumAllPredictionReduceMinCostCount) / float64(recordsCount)
	sorter := &util.Sorter{
		LenFunc: func() int {
			return len(JobsCount2SummaryRecord)
		},
		LessFunc: func(i, j int) bool {
			left := JobsCount2SummaryRecord[i].(*specificJobsCountSummaryRecord)
			right := JobsCount2SummaryRecord[j].(*specificJobsCountSummaryRecord)
			return left.JobsCount < right.JobsCount
		},
		SwapFunc: func(i, j int) {
			t := JobsCount2SummaryRecord[i]
			JobsCount2SummaryRecord[i] = JobsCount2SummaryRecord[j]
			JobsCount2SummaryRecord[j] = t
		},
	}
	sort.Sort(sorter)
	m.Record.JobsCount2SummaryRecord = JobsCount2SummaryRecord
	return m.Record
}

func (m *BranchAndBoundTemplate) MinCost(params *MinCostParams) (float64, []types.Job) {
	record := &BranchAndBoundCallRecord{
		JobsCount: len(params.Jobs),
	}
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		record.Duration = duration
		m.RecordMu.Lock()
		defer m.RecordMu.Unlock()
		if _, ok := m.Record.CallRecords[record.JobsCount]; !ok {
			m.Record.CallRecords[record.JobsCount] = make([]*BranchAndBoundCallRecord, 0, 1024)
		}
		m.Record.CallRecords[record.JobsCount] = append(m.Record.CallRecords[record.JobsCount], record)
	}()
	costSolver := params.CostSolver
	gpu := params.GPU
	copiedJobs := jobs_util.GetJobsSliceUtil().Copy(params.Jobs)
	params.Jobs = copiedJobs
	// 如果ddl被违反了，则使用分支限界法进行搜索具有最优cost的解。
	// Node 表示分支限界法中的一个节点。
	nodes := m.impl.initNodes(params)
	record.ExpandNodesCount = len(nodes)
	isAnswerNode := func(node *BranchAndBoundNode) bool {
		return len(node.jobs) == len(params.Jobs)
	}
	// 记录当前的一个cost上界。记录了当前最优的cost（可能是估计的）。如果在搜索节点时，发现当前节点的cost已经>=该上界，则放弃当前节点（剪枝）
	minCost := math.Inf(1)
	// optimus 记录最优解的jobs slice。
	var optimus []types.Job = nil
	for _, node := range nodes {
		if isAnswerNode(node) {
			minCost = math.Min(minCost, math.Min(node.cost, minCost))
			optimus = node.jobs
			continue
		}
		minCost = math.Min(minCost, node.predictCost)
	}
	// 构建最小堆，用于LC的分支限界搜索方法，每次选取成本最小的节点，但是在估计成本上，有两种策略。
	// 目前的策略是使用，每个队列包含的部分任务的cost作为LC节点的选取标准。
	// 实际上，还可以采取使用每个节点的预估完整cost作为标准。实际哪个效率更高，需要测试。
	minHeap := util.HeapSorter{
		LenFunc: func() int {
			return len(nodes)
		},
		LessFunc: func(i, j int) bool {
			// 这里指定了LC的节点选取规则。
			switch m.LCStandard {
			case BranchAndBoundLCStandardPartialCost:
				return nodes[i].cost < nodes[j].cost
			case BranchAndBoundLCStandardPredictCost:
				return nodes[i].predictCost < nodes[j].predictCost
			default:
				panic("Unsupported LCStandard")
			}
		},
		SwapFunc: func(i, j int) {
			o := nodes[i]
			nodes[i] = nodes[j]
			nodes[j] = o
		},
		PushFunc: func(x interface{}) {
			nodes = append(nodes, x.(*BranchAndBoundNode))
		},
		PopFunc: func() interface{} {
			last := len(nodes) - 1
			node := nodes[last]
			nodes = nodes[:last]
			return node
		},
	}
	heap.Init(minHeap)
	// 当还存在活节点时，从heap中找出最小成本的节点，进行扩展。
	for minHeap.Len() > 0 {
		expandingNode := heap.Pop(minHeap).(*BranchAndBoundNode)
		// 首先查看当前节点是否是一个答案节点。
		if len(expandingNode.jobs) == len(params.Jobs) {
			// 如果是，则更新minCost和optimus
			if expandingNode.cost <= minCost {
				minCost = expandingNode.cost
				optimus = expandingNode.jobs
			}
			continue
		}

		// 如果是非答案节点，再次检查自己的cost是否比minCost大。
		if expandingNode.cost > minCost {
			// 剪枝
			record.TotalCutCount++
			continue
		}
		// 如果cost下界比minCost差，则剪枝。
		if expandingNode.cHat > minCost {
			// 剪枝
			record.TotalCutCount++
			record.CHatCutCount++
			continue
		}

		// 如果是非答案节点，则首先建立一个minCostNode当前包含的job的集合，
		// 用于查询当前有哪些任务还没在这个队列里。
		jobNamesInCurrNode := make(map[types.JobName]bool)
		for _, job := range expandingNode.jobs {
			jobNamesInCurrNode[job.JobName()] = true
		}
		// 遍历全部job的队列，找到那些不在当前Node包含的jobs中的那些job，尝试对它们进行扩展生成新的活节点。
		for _, job := range params.Jobs {
			if _, ok := jobNamesInCurrNode[job.JobName()]; ok {
				continue
			}
			// 找到了，则尝试对它扩展。
			newJob := job
			// 扩展得到新的一系列的jobs序列。
			newJobsSequences := m.impl.expandNode(expandingNode, newJob, params)
			record.ExpandNodesCount += len(newJobsSequences)
			if len(newJobsSequences) == 0 {
				record.TotalCutCount++
				record.ExpandNothingCutCount++
				continue
			}
			for _, newJobs := range newJobsSequences {
				newJobs := newJobs
				costResp := costSolver.Cost(gpu, newJobs)
				if costResp.Cost > minCost {
					// 当前不完全的jobs队列的cost已经大于minCost了，
					// 那么它在后续继续添加任务，cost只可能增加。所以剪枝。
					record.AfterExpandCutCount++
					record.TotalCutCount++
					continue
				}
				cHat, predictValidCost, predictValidOptimus := func() (float64, float64, []types.Job) {
					// 这里重复利用下jobNamesInNode，避免重复劳动。
					jobNamesInCurrNode[newJob.JobName()] = true
					// defer 千万别忘了将该jobName从该set中删除。
					defer delete(jobNamesInCurrNode, newJob.JobName())
					// 找出剩余的不在该扩展后节点的job，组成otherJobs
					otherJobs := make([]types.Job, 0, len(params.Jobs)-len(jobNamesInCurrNode))
					for _, otherJob := range params.Jobs {
						if _, ok := jobNamesInCurrNode[otherJob.JobName()]; !ok {
							otherJobs = append(otherJobs, otherJob)
						}
					}
					return m.impl.predict(newJobs, otherJobs, params)
				}()

				// 尝试更新minCost
				if predictValidCost <= minCost {
					minCost = predictValidCost
					if predictValidCost < minCost {
						record.PredictionReduceMinCostCount++
					}
					// 如果minCost更新了，同时该predictOptimus就是这条分支的最优解，则更新全局最优解
					// 并且，在这时不需要添加活节点，因为这条分支的最优解已经求出。
					if predictValidOptimus != nil {
						optimus = predictValidOptimus
					}
				}

				// 如果找出这条分支的最优解，则不添加活节点，直接跳过这条分支的剩余节点。
				if predictValidOptimus != nil {
					record.PredictionIsOptimusCutCount++
					record.TotalCutCount++
					continue
				}

				newNode := &BranchAndBoundNode{
					jobs:        newJobs,
					cost:        costResp.Cost,
					predictCost: predictValidCost,
					cHat:        cHat,
				}
				// 将扩展好的节点加入到最小堆。
				heap.Push(minHeap, newNode)
			}
		}
	}
	if optimus == nil {
		panic("optimus == nil")
	}
	return minCost, optimus
}

type BranchAndBoundAllPermutation struct {
	*BranchAndBoundTemplate
}

func (m *BranchAndBoundAllPermutation) String() string {
	return fmt.Sprintf("BranchAndBoundAllPermutation[LCStandard=%s]", m.LCStandard)
}

func (m *BranchAndBoundAllPermutation) initNodes(params *MinCostParams) []*BranchAndBoundNode {
	return []*BranchAndBoundNode{
		{
			[]types.Job{}, math.Inf(1), math.Inf(1), math.Inf(1),
		},
	}
}

func (m *BranchAndBoundAllPermutation) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *MinCostParams) [][]types.Job {
	newJobs := make([]types.Job, len(currNode.jobs), len(currNode.jobs)+1)
	copy(newJobs, currNode.jobs)
	newJobs = append(newJobs, newJob)
	return [][]types.Job{newJobs}
}

func (m *BranchAndBoundAllPermutation) predict(
	newJobs []types.Job,
	otherJobs []types.Job,
	params *MinCostParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job) {

	// 将他们按照SRTF排序。
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(params.GPU.Type(), otherJobs)
	// 构建新预测的完整jobs队列。这个队列的前面是当前已经扩展的部分节点包含的jobs
	// 后半部分是还未加入到当前解中的，其他的jobs。这些jobs按照SRTF排序了。
	predictJobList := make([]types.Job, len(newJobs))
	copy(predictJobList, newJobs)
	predictJobList = append(predictJobList, otherJobs...)
	predictCostResp := params.CostSolver.Cost(params.GPU, predictJobList)

	// 计算这条分支的成本下界
	cHat = func() float64 {
		// 目前cHat的计算方法：将剩余任务按照SRTF排序后，计算cost，但是不考虑ddl违反带来的cost。
		// 这里可以直接利用predictJobList，因为他们的排序方案是一样的。
		// 这个jctCost是当前序列最想达到的，而几乎不可能达到的。
		return predictCostResp.JCTCost
	}()

	// 如果otherJobs这部分任务没有ddl违反，则该predict的job list就是当前分支的最优解
	// 这是一般剪枝函数无法做到的，因为在我们的问题中，如果ddl没有违反，则SRTF就一定是一个最优的解。
	// 所以，这种方法能够快速将一个分支的最优解求出，比剪枝函数还要牛。
	otherJobsContainsViolated := len(jobs_util.GetJobsSliceUtil().Intersects(otherJobs, predictCostResp.DDLViolatedJobs)) > 0
	if !otherJobsContainsViolated {
		return cHat, predictCostResp.Cost, predictJobList
	}

	// TODO 目前将otherJobs经过SRTF排序后的cost直接作为最小成本上界。
	// TODO 但实际上，也可以通过其他算法，如启发式的贪心算法，获得一个更好的最小成本上界。
	// TODO 后续添加其他算法在这个位置。
	U := predictCostResp.Cost

	return cHat, U, nil
}

type BranchAndBoundFixNonDDL struct {
	*BranchAndBoundAllPermutation
}

func (b *BranchAndBoundFixNonDDL) String() string {
	return fmt.Sprintf("BranchAndBoundFixNonDDL[LCStandard=%s]", b.LCStandard)
}

func (b *BranchAndBoundFixNonDDL) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *MinCostParams) [][]types.Job {
	jobsUtil := jobs_util.GetJobsSliceUtil()
	if !jobsUtil.JobHasDDL(newJob) {
		for _, job := range currNode.jobs {
			if !jobsUtil.JobHasDDL(job) && job.RemainingDuration(params.GPU.Type()) > newJob.RemainingDuration(params.GPU.Type()) {
				return nil
			}
		}
	}
	newJobs := make([]types.Job, len(currNode.jobs), len(currNode.jobs)+1)
	copy(newJobs, currNode.jobs)
	newJobs = append(newJobs, newJob)
	return [][]types.Job{newJobs}
}

type BranchAndBoundDDLInsertion struct {
	*BranchAndBoundTemplate
}

func (b *BranchAndBoundDDLInsertion) String() string {
	return fmt.Sprintf("BranchAndBoundDDLInsertion[LCStandard=%s]", b.LCStandard)
}

func (b *BranchAndBoundDDLInsertion) initNodes(params *MinCostParams) []*BranchAndBoundNode {
	noDDLJobs := make([]types.Job, 0, len(params.Jobs))
	withDDLJobs := make([]types.Job, 0, len(params.Jobs))
	for _, job := range params.Jobs {
		if math.IsInf(float64(job.JobMeta().DDL()), 1) {
			// 该任务没有ddl限制，可以不需考虑它们之间的相对位置。
			noDDLJobs = append(noDDLJobs, job)
		} else {
			withDDLJobs = append(withDDLJobs, job)
		}
	}
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(params.GPU.Type(), noDDLJobs)
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(params.GPU.Type(), withDDLJobs)
	costResp := params.CostSolver.Cost(params.GPU, noDDLJobs)
	cHat, predictValidCost, _ := b.predict(noDDLJobs, withDDLJobs, params)
	initNodes := []*BranchAndBoundNode{
		{
			noDDLJobs, costResp.Cost, predictValidCost, cHat,
		},
	}
	return initNodes
}

// expandNode 使用插空位的方式，生成扩展节点。
func (b *BranchAndBoundDDLInsertion) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *MinCostParams) [][]types.Job {
	newJobsSequence := make([][]types.Job, 0, len(currNode.jobs)+1)
	for i := 0; i < len(currNode.jobs)+1; i++ {
		newJobs := make([]types.Job, len(currNode.jobs)+1, len(currNode.jobs)+1)
		copy(newJobs[:i], currNode.jobs[:i])
		copy(newJobs[i+1:], currNode.jobs[i:])
		newJobs[i] = newJob
		newJobsSequence = append(newJobsSequence, newJobs)
	}
	return newJobsSequence
}

func (b *BranchAndBoundDDLInsertion) predict(
	newJobs []types.Job,
	otherJobs []types.Job,
	params *MinCostParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job) {
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(params.GPU.Type(), otherJobs)
	// 构建新预测的完整jobs队列。
	// 设计一个简单的贪心预测方法：在DDL Insertion方案中，otherJobs都是具有DDL的任务。
	// 将它们看做一个整体，从左到右插入到已有任务的空位中，向右移动，直到发现有任务违反了DDL，
	// 或者Cost上升，则停止。
	predictJobList := func() []types.Job {
		var bestList []types.Job = nil
		var bestCostResp *Resp = nil
		for i := 0; i < len(newJobs); i++ {
			// i表示插入到newJobs中的位置。
			resultList := make([]types.Job, len(newJobs)+len(otherJobs))
			copy(resultList[:i], newJobs[:i])
			copy(resultList[i:i+len(otherJobs)], otherJobs)
			copy(resultList[i+len(otherJobs):], newJobs[i:])
			resp := params.CostSolver.Cost(params.GPU, resultList)
			if bestList == nil {
				bestList = resultList
				bestCostResp = resp
				continue
			}
			if resp.Cost > bestCostResp.Cost {
				return bestList
			}
			if resp.DDLViolated {
				return bestList
			}
			if resp.Cost < bestCostResp.Cost {
				bestList = resultList
				bestCostResp = resp
			}
		}
		if bestList == nil {
			return otherJobs
		}
		return bestList
	}()
	predictCostResp := params.CostSolver.Cost(params.GPU, predictJobList)
	// 计算这条分支的成本下界
	cHat = func() float64 {
		// 在本方法中，cHat可以使用已经在节点里的全部任务的Cost表示。
		// 因为当加入新的任务时，cost必然会上升，所以这种表示方法能够肯定的得到一个下界。
		resp := params.CostSolver.Cost(params.GPU, newJobs)
		return resp.Cost
	}()

	U := predictCostResp.Cost
	// TODO 也可以通过启发式的贪心算法，获得一个更好的最小成本上界。
	// TODO 后续添加其他算法在这个位置。

	return cHat, U, nil
}
