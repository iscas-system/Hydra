package cost

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/schedulers/types"
	"DES-go/util"
	"container/heap"
	"fmt"
	"math"
)

type MinCostAlgoByBranchAndBound interface {
	MinCostAlgo

	// initNodes
	// 初始化分支限界的节点。
	initNodes(params *BranchAndBoundAlgoParams) []*BranchAndBoundNode
	// expandNode
	// 当遇到一个未在当前节点内的新节点时，扩展它，返回多个任务序列。
	expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *BranchAndBoundAlgoParams) [][]types.Job
	// predict
	// 对当前的扩展节点计算一个预测的cost，将其他未在队列的jobs都加入进来，并按照SJF排序，计算一个cost。它必定>=最优解的cost。
	// 如果预测的cost小于minCost，则将minCost更新为predictCost，这样能够任意一个答案节点计算出来之前，获得一个cost上界。
	// 这个predictValidCost是一个可行解，但不一定是最优解，所以为了在扩展节点时高效剪枝，还需要计算一个成本下界cHat，用来做限界函数。
	// cHat为当前这个分支上，成本的下界，即这个序列的成本不可能 <= cHat。如果cHat都比minCost大，那么这条分支就不可能取最优解。
	predict(newJobs []types.Job,
		otherJobs []types.Job,
		params *BranchAndBoundAlgoParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job)

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

type BranchAndBoundAlgoParams struct {
	CostSolver Solver
	GPU        types.GPU
	Jobs []types.Job
}

type BranchAndBoundLCStandard string
type BranchAndBoundAlgoType int

const (
	BranchAndBoundLCStandardPartialCost BranchAndBoundLCStandard = "PartialCost"
	BranchAndBoundLCStandardPredictCost BranchAndBoundLCStandard = "PredictCost"

	BranchAndBoundAlgoTypeAllPermutation BranchAndBoundAlgoType = 0
	BranchAndBoundAlgoTypeDDLInsertion   BranchAndBoundAlgoType = 1
	BranchAndBoundAlgoTypeFixNonDDL BranchAndBoundAlgoType = 2
)

type BranchAndBoundTemplate struct {
	LCStandard BranchAndBoundLCStandard
	impl       MinCostAlgoByBranchAndBound
}

func NewBranchAndBoundAlgo(LCStandard BranchAndBoundLCStandard, algoType BranchAndBoundAlgoType) MinCostAlgoByBranchAndBound {
	template := &BranchAndBoundTemplate{
		LCStandard: LCStandard,
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

func (m *BranchAndBoundTemplate) MinCostT2(params *BranchAndBoundAlgoParams) (float64, []types.Job) {
	costSolver := params.CostSolver
	jobs := params.Jobs
	gpu := params.GPU

	copiedJobs := jobs_util.GetJobsSliceUtil().Copy(jobs)
	// 如果ddl被违反了，则使用分支限界法进行搜索具有最优cost的解。
	// branch and bound，顺带复习了算法，很不戳。
	// Node 表示分支限界法中的一个节点。
	type Node struct {
		jobs []types.Job
		// Cost 这个节点存储的部分jobs的成本。它是不完全的，必定小于将完整的job都加入后的cost。
		cost float64
		// predictCost 如果将不在这个节点的jobs都加入进来，并按照SJF进行排序，给出一个预测的Cost。
		// 它可以用于快速判断minCost的下界，使得当没有最终答案节点算出时，也能修正minCost的值，达到快速剪枝的目的。
		predictCost float64
		// cHat 如果将不在这个节点的jobs都加入进来，那么我们能计算出一个cost的下界，保证我们的解不可能比它更好。
		// 那么在展开节点时，可以依据它来剪枝：若cHat都比当前的最优解minCost要差，那么当前分支不可能产生比minCost更优的解。
		cHat float64
	}
	nodes := make([]*Node, 0, 1)
	// 初始节点，没有任何元素的一个节点。
	nodes = append(nodes, &Node{
		[]types.Job{}, math.Inf(1), math.Inf(1), math.Inf(1),
	})
	// 记录当前的一个cost上界。记录了当前最优的cost（可能是估计的）。如果在搜索节点时，发现当前节点的cost已经>=该上界，则放弃当前节点（剪枝）
	minCost := math.Inf(1)
	// optimus 记录最优解的jobs slice。
	var optimus []types.Job = nil
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
			nodes = append(nodes, x.(*Node))
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
	// fmt.Printf("copiedJobs=[%s]\n", util.Pretty(copiedJobs))
	for minHeap.Len() > 0 {
		expandingNode := heap.Pop(minHeap).(*Node)
		// 首先查看当前节点是否是一个答案节点。
		if len(expandingNode.jobs) == len(copiedJobs) {
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
			continue
		}
		// 如果cost下界比minCost差，则剪枝。
		if expandingNode.cHat > minCost {
			// 剪枝
			continue
		}

		// 如果是非答案节点，则首先建立一个minCostNode当前包含的job的集合，
		// 用于查询当前有哪些任务还没在这个队列里。
		jobNamesInNode := make(map[types.JobName]bool)
		for _, job := range expandingNode.jobs {
			jobNamesInNode[job.JobName()] = true
		}
		// 遍历全部job的队列，找到那些不在当前Node包含的jobs中的那些job，尝试对它们进行扩展生成新的活节点。
		for _, job := range copiedJobs {
			if _, ok := jobNamesInNode[job.JobName()]; ok {
				continue
			}
			// 找到了，则尝试对它扩展。
			newJob := job
			newJobs := make([]types.Job, len(expandingNode.jobs))
			copy(newJobs, expandingNode.jobs)
			newJobs = append(newJobs, newJob)
			costResp := costSolver.Cost(gpu, newJobs)
			if costResp.Cost > minCost {
				// 当前不完全的jobs队列的cost已经大于minCost了，
				// 那么它在后续继续添加任务，cost只可能增加。所以剪枝。
				continue
			}
			// 对当前的扩展节点计算一个预测的cost，将其他未在队列的jobs都加入进来，并按照SJF排序，计算一个cost。它必定>=最优解的cost。
			// 如果预测的cost小于minCost，则将minCost更新为predictCost，这样能够任意一个答案节点计算出来之前，获得一个cost上界。
			// 这个predictValidCost是一个可行解，但不一定是最优解，所以为了在扩展节点时高效剪枝，还需要计算一个成本下界cHat，用来做限界函数。
			// cHat为当前这个分支上，成本的下界，即这个序列的成本不可能 <= cHat。如果cHat都比minCost大，那么这条分支就不可能取最优解。
			cHat, predictValidCost, predictValidOptimus := func() (float64, float64, []types.Job) {
				// 这里重复利用下jobNamesInNode，避免重复劳动。
				jobNamesInNode[newJob.JobName()] = true
				// defer 千万别忘了将该jobName从该set中删除。
				defer delete(jobNamesInNode, newJob.JobName())
				// 找出剩余的不在该扩展后节点的job，组成otherJobs
				otherJobs := make([]types.Job, 0, len(copiedJobs)-len(jobNamesInNode))
				for _, otherJob := range copiedJobs {
					if _, ok := jobNamesInNode[otherJob.JobName()]; !ok {
						otherJobs = append(otherJobs, otherJob)
					}
				}

				// 将他们按照SRTF排序。
				jobs_util.GetJobsSliceUtil().ReorderToSRTF(gpu.Type(), otherJobs)
				// 构建新预测的完整jobs队列。这个队列的前面是当前已经扩展的部分节点包含的jobs
				// 后半部分是还未加入到当前解中的，其他的jobs。这些jobs按照SRTF排序了。
				predictJobList := make([]types.Job, len(newJobs))
				copy(predictJobList, newJobs)
				predictJobList = append(predictJobList, otherJobs...)
				predictCostResp := costSolver.Cost(gpu, predictJobList)

				// 计算这条分支的成本下界
				cHat := func() float64 {
					// 目前cHat的计算方法：将剩余任务按照SRTF排序后，计算cost，但是不考虑ddl违反带来的cost。
					// 这里可以直接利用predictJobList，因为他们的排序方案是一样的。
					// 这个jctCost是当前序列最想达到的
					cHat := predictCostResp.JCTCost
					return cHat
				}()

				// 如果otherJobs这部分任务没有ddl违反，则该predict的job list就是当前分支的最优解
				// 这是一般剪枝函数无法做到的，因为在我们的问题中，如果ddl没有违反，则SRTF就一定是一个最优的解。
				// 所以，这种方法能够快速将一个分支的最优解求出，比剪枝函数还要牛。
				otherJobsContainsViolated := len(jobs_util.GetJobsSliceUtil().Intersects(otherJobs, predictCostResp.DDLViolatedJobs)) > 0
				if otherJobsContainsViolated {
					return cHat, predictCostResp.Cost, predictJobList
				}

				// TODO 目前将otherJobs经过SRTF排序后的cost直接作为最小成本上界。
				// TODO 但实际上，也可以通过其他算法，如启发式的贪心算法，获得一个更好的最小成本上界。
				// TODO 后续添加其他算法在这个位置，
				U := predictCostResp.Cost

				return cHat, U, nil
			}()

			// 尝试更新minCost
			if predictValidCost < minCost {
				minCost = predictValidCost
				// 如果minCost更新了，同时该predictOptimus就是这条分支的最优解，则更新全局最优解
				// 并且，在这时不需要添加活节点，因为这条分支的最优解已经求出。
				if predictValidOptimus != nil {
					optimus = predictValidOptimus
				}
			}

			// 如果找出这条分支的最优解，则不添加活节点，直接跳过这条分支的剩余节点。
			if predictValidOptimus != nil {
				continue
			}

			newNode := &Node{
				jobs:        newJobs,
				cost:        costResp.Cost,
				predictCost: predictValidCost,
				cHat:        cHat,
			}
			// 将扩展好的节点加入到最小堆。
			heap.Push(minHeap, newNode)
		}
	}
	if optimus == nil {
		panic("optimus == nil")
	}
	return minCost, optimus
}

func (m *BranchAndBoundTemplate) initNodes(params *BranchAndBoundAlgoParams) []*BranchAndBoundNode {
	panic("Template method should not be called.")
}

func (m *BranchAndBoundTemplate) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *BranchAndBoundAlgoParams) [][]types.Job {
	panic("Template method should not be called.")
}

func (m *BranchAndBoundTemplate) predict(
	jobNamesInCurrNode map[types.JobName]bool,
	newJob types.Job,
	newJobs []types.Job,
	params *BranchAndBoundAlgoParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job) {
	panic("Template method should not be called.")
}

func (m *BranchAndBoundTemplate) MinCost(params *BranchAndBoundAlgoParams) (float64, []types.Job) {
	costSolver := params.CostSolver
	gpu := params.GPU
	copiedJobs := jobs_util.GetJobsSliceUtil().Copy(params.Jobs)
	params.Jobs = copiedJobs
	// 如果ddl被违反了，则使用分支限界法进行搜索具有最优cost的解。
	// Node 表示分支限界法中的一个节点。
	nodes := m.impl.initNodes(params)
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
	// fmt.Printf("copiedJobs=[%s]\n", util.Pretty(copiedJobs))
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
			continue
		}
		// 如果cost下界比minCost差，则剪枝。
		if expandingNode.cHat > minCost {
			// 剪枝
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
			for _, newJobs := range newJobsSequences {
				newJobs := newJobs
				// fmt.Printf("expanding Node %s, newJob %s, newJobs %s\n", util.Pretty(expandingNode), util.Pretty(newJob), util.Pretty(newJobs))
				costResp := costSolver.Cost(gpu, newJobs)
				if costResp.Cost > minCost {
					// 当前不完全的jobs队列的cost已经大于minCost了，
					// 那么它在后续继续添加任务，cost只可能增加。所以剪枝。
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
				} ()

				// 尝试更新minCost
				if predictValidCost < minCost {
					minCost = predictValidCost
					// 如果minCost更新了，同时该predictOptimus就是这条分支的最优解，则更新全局最优解
					// 并且，在这时不需要添加活节点，因为这条分支的最优解已经求出。
					if predictValidOptimus != nil {
						optimus = predictValidOptimus
					}
				}

				// 如果找出这条分支的最优解，则不添加活节点，直接跳过这条分支的剩余节点。
				if predictValidOptimus != nil {
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

func (m *BranchAndBoundAllPermutation) initNodes(params *BranchAndBoundAlgoParams) []*BranchAndBoundNode {
	return []*BranchAndBoundNode{
		{
			[]types.Job{}, math.Inf(1), math.Inf(1), math.Inf(1),
		},
	}
}

func (m *BranchAndBoundAllPermutation) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *BranchAndBoundAlgoParams) [][]types.Job {
	newJobs := make([]types.Job, len(currNode.jobs), len(currNode.jobs) + 1)
	copy(newJobs, currNode.jobs)
	newJobs = append(newJobs, newJob)
	return [][]types.Job{newJobs}
}

func (m *BranchAndBoundAllPermutation) predict(
	newJobs []types.Job,
	otherJobs []types.Job,
	params *BranchAndBoundAlgoParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job) {

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
	if otherJobsContainsViolated {
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

func (b *BranchAndBoundFixNonDDL) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *BranchAndBoundAlgoParams) [][]types.Job {
	jobsUtil := jobs_util.GetJobsSliceUtil()
	if !jobsUtil.JobHasDDL(newJob) {
		for _, job := range currNode.jobs {
			if !jobsUtil.JobHasDDL(job) && job.RemainingDuration(params.GPU.Type()) > newJob.RemainingDuration(params.GPU.Type()) {
				return nil
			}
		}
	}
	newJobs := make([]types.Job, len(currNode.jobs), len(currNode.jobs) + 1)
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

func (b *BranchAndBoundDDLInsertion) initNodes(params *BranchAndBoundAlgoParams) []*BranchAndBoundNode {
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
func (b *BranchAndBoundDDLInsertion) expandNode(currNode *BranchAndBoundNode, newJob types.Job, params *BranchAndBoundAlgoParams) [][]types.Job {
	newJobsSequence := make([][]types.Job, 0, len(currNode.jobs) + 1)
	for i := 0; i < len(currNode.jobs) + 1; i++ {
		newJobs := make([]types.Job, len(currNode.jobs) + 1, len(currNode.jobs) + 1)
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
	params *BranchAndBoundAlgoParams) (cHat float64, predictValidCost float64, predictValidOptimus []types.Job) {
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
			resultList := make([]types.Job, len(newJobs) + len(otherJobs))
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
	} ()
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