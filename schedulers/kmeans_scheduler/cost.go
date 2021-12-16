package kmeans_scheduler

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/simulator"
	"DES-go/util"
	"container/heap"
	"math"
	"strconv"
	"strings"
	"sync"
)

// costSolverCommon 定义了成本计算的公用类型。
type costSolverCommon struct {
	gpuCluster *simulator.Cluster
	costMemo   *sync.Map // map[string]*costResp
}

func newCostSolverCommon(gpuCluster *simulator.Cluster) *costSolverCommon {
	return &costSolverCommon{
		gpuCluster: gpuCluster,
		costMemo:   &sync.Map{},
	}
}

func (c *costSolverCommon) costMemoKey(gpu *simulator.GPU, jobs []*simulator.Job) string {
	builder := &strings.Builder{}
	// gpu info
	builder.WriteString("GPU:")
	builder.WriteString(gpu.String())
	writeJob := func(job *simulator.Job) {
		builder.WriteString(string(job.JobName()))
		builder.WriteString(strconv.FormatFloat(job.RemainingRatio(), 'f', 6, 64))
		builder.WriteByte('-')
	}
	builder.WriteString("Jobs:")
	for _, job := range jobs {
		writeJob(job)
	}
	return builder.String()
}

// CostSolver 定义成本计算方式。
type CostSolver interface {
	Cost(gpu *simulator.GPU, jobs []*simulator.Job) *costResp
}

type CostSolverMaker func(gpuCluster *simulator.Cluster) CostSolver

type DDLCostType int

const (
	DDLCostTypeStrict DDLCostType = 0 // Strict，表示严格的DDL要求，即只要违约了DDL一点点，就认为非常严重。
	DDLCostTypeSoft   DDLCostType = 1 // Soft，表示较为宽松的DDL要求。
)

type costResp struct {
	cost            float64
	jctCost         float64
	ddlCost         float64
	ddlViolatedJobs []*simulator.Job
	ddlViolated     bool
}

// ---------------------------------------- SimpleAddCostSolver ---------------------------------------

// SimpleAddCostSolver 简单的将JCT与DDL violation相加作为cost。参数可以指定ddl系数等。
type SimpleAddCostSolver struct {
	*costSolverCommon
	ddlCostType              DDLCostType
	ddlStrictCostCoefficient float64
}

func NewSimpleAddCostSolverMaker(ddlCostType DDLCostType, ddlStrictCostCoefficient float64) CostSolverMaker {
	return func(gpuCluster *simulator.Cluster) CostSolver {
		return &SimpleAddCostSolver{
			costSolverCommon:         newCostSolverCommon(gpuCluster),
			ddlCostType:              ddlCostType,
			ddlStrictCostCoefficient: ddlStrictCostCoefficient,
		}
	}
}

// Cost 本函数为该算法的核心部分，它定义在一个gpu上的一组排序号的jobs，它的总代价是多大。
// 目前将代价分为两部分，一部分是JCT，另一部分是DDL违约。
// 那么JCT就按照累加求和即可，而DDL作为更为首要的要求，可以使用一个高倍的系数，乘以每个违约job的违约时长，使得它比JCT更重要。
// 那么这里也可以加入soft DDL的模式，即当job只违反了一点点DDL时，不认为它非常严重。
// 返回值: 分别返回，代价的大小（float64），以及是否存在DDL违反（bool）。
func (c *SimpleAddCostSolver) Cost(gpu *simulator.GPU, jobs []*simulator.Job) *costResp {
	// 如果有过相同调用，则直接返回记录的结果。
	memoKey := c.costMemoKey(gpu, jobs)
	if memorized, ok := c.costMemo.Load(memoKey); ok {
		return memorized.(*costResp)
	}

	jctOffset := c.gpuCluster.Now()
	// 考虑到非抢占式调度，要将当前正在运行的任务剩余运行时间考虑进来。
	runningJob := c.gpuCluster.CurrRunningJob(gpu.ID())
	if runningJob != nil {
		jctOffset += simulator.Time(runningJob.RemainingDuration(gpu.Type()))
	}
	ddlViolatedJobs := make([]*simulator.Job, 0)

	// 第一步，计算每个任务的jct，以及每个任务违反ddl的时长。
	JCTs, ddlViolations := func() ([]simulator.Time, []simulator.Duration) {
		JCTs := make([]simulator.Time, 0, len(jobs))
		ddlViolations := make([]simulator.Duration, 0, len(jobs))
		jctOffset := jctOffset
		for _, job := range jobs {
			// 此处是预测job的JCT，不是计算已经完成的任务的JCT，所以不可以调用job.JCT()，因为job.JCT()只有当任务实际已经完成时才能返回结果。
			currJobJCT := jctOffset + simulator.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
			jctOffset = currJobJCT
			JCTs = append(JCTs, currJobJCT)
			if currJobJCT > job.JobMeta().DDL() {
				ddlViolations = append(ddlViolations, simulator.Duration(currJobJCT-job.JobMeta().DDL()))
				ddlViolatedJobs = append(ddlViolatedJobs, job)
			} else {
				ddlViolations = append(ddlViolations, 0)
			}
		}
		return JCTs, ddlViolations
	}()

	// 第二步，计算jct带来的cost。
	JCTCost := func() float64 {
		// 目前，简单的将JCT求和后的值作为JCT Costs。这里在后面可以进行修改，比如增加一些系数。
		interfaceJCTs := make([]interface{}, len(JCTs))
		for idx, jct := range JCTs {
			interfaceJCTs[idx] = jct
		}
		return util.SumFloat64(func(item interface{}) float64 {
			return float64(item.(simulator.Time))
		}, interfaceJCTs...)
	}()

	// 第三步，计算DDL violation带来的cost。
	DDLCost := func() float64 {
		// 计算每个job的ddl violation cost，并求和
		ddlViolationCosts := make([]interface{}, 0, len(ddlViolations))
		for _, violation := range ddlViolations {
			if violation <= 0 {
				continue
			}
			switch c.ddlCostType {
			case DDLCostTypeStrict:
				ddlViolationCosts = append(ddlViolationCosts, c.ddlStrictCostCoefficient*float64(violation))
			case DDLCostTypeSoft:
				// TODO
			default:
				panic("Unsupported ddlCostType")
			}
		}
		return util.SumFloat64(func(item interface{}) float64 {
			return item.(float64)
		}, ddlViolationCosts...)
	}()

	// 第四步，求和即可。这里实际上还是可以继续调参，比如对他们分别乘以系数。
	costResp := &costResp{
		cost:            JCTCost + DDLCost,
		jctCost:         JCTCost,
		ddlCost:         DDLCost,
		ddlViolated:     DDLCost > 0,
		ddlViolatedJobs: ddlViolatedJobs,
	}
	c.costMemo.Store(memoKey, costResp)
	return costResp
}

type MinCostAlgo func(costSolver CostSolver, gpu *simulator.GPU, jobs []*simulator.Job) (float64, []*simulator.Job)

type MinCostBranchAndBoundLCStandard int

const (
	MinCostBranchAndBoundLCStandardPartialCost MinCostBranchAndBoundLCStandard = 0
	MinCostBranchAndBoundLCStandardPredictCost MinCostBranchAndBoundLCStandard = 1
)

func NewMinCostByBranchAndBoundAlgo(LCStandard MinCostBranchAndBoundLCStandard) MinCostAlgo {
	return func(costSolver CostSolver, gpu *simulator.GPU, originalJobs []*simulator.Job) (float64, []*simulator.Job) {
		copiedJobs := jobs_util.GetJobsSliceUtil().Copy(originalJobs)
		// 如果ddl被违反了，则使用分支限界法进行搜索具有最优cost的解。
		// branch and bound，顺带复习了算法，很不戳。
		// Node 表示分支限界法中的一个节点。
		type Node struct {
			jobs []*simulator.Job
			// cost 这个节点存储的部分jobs的成本。它是不完全的，必定小于将完整的job都加入后的cost。
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
			[]*simulator.Job{}, math.Inf(1), math.Inf(1), math.Inf(1),
		})
		// 记录当前的一个cost上界。记录了当前最优的cost（可能是估计的）。如果在搜索节点时，发现当前节点的cost已经>=该上界，则放弃当前节点（剪枝）
		minCost := math.Inf(1)
		// optimus 记录最优解的jobs slice。
		var optimus []*simulator.Job = nil
		// 构建最小堆，用于LC的分支限界搜索方法，每次选取成本最小的节点，但是在估计成本上，有两种策略。
		// 目前的策略是使用，每个队列包含的部分任务的cost作为LC节点的选取标准。
		// 实际上，还可以采取使用每个节点的预估完整cost作为标准。实际哪个效率更高，需要测试。
		minHeap := util.HeapSorter{
			LenFunc: func() int {
				return len(nodes)
			},
			LessFunc: func(i, j int) bool {
				// 这里指定了LC的节点选取规则。
				switch LCStandard {
				case MinCostBranchAndBoundLCStandardPartialCost:
					return nodes[i].cost < nodes[j].cost
				case MinCostBranchAndBoundLCStandardPredictCost:
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
			jobNamesInNode := make(map[simulator.JobName]bool)
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
				newJobs := make([]*simulator.Job, len(expandingNode.jobs))
				copy(newJobs, expandingNode.jobs)
				newJobs = append(newJobs, newJob)
				costResp := costSolver.Cost(gpu, newJobs)
				if costResp.cost > minCost {
					// 当前不完全的jobs队列的cost已经大于minCost了，
					// 那么它在后续继续添加任务，cost只可能增加。所以剪枝。
					continue
				}
				// 对当前的扩展节点计算一个预测的cost，将其他未在队列的jobs都加入进来，并按照SJF排序，计算一个cost。它必定>=最优解的cost。
				// 如果预测的cost小于minCost，则将minCost更新为predictCost，这样能够任意一个答案节点计算出来之前，获得一个cost上界。
				// 这个predictValidCost是一个可行解，但不一定是最优解，所以为了在扩展节点时高效剪枝，还需要计算一个成本下界cHat，用来做限界函数。
				// cHat为当前这个分支上，成本的下界，即这个序列的成本不可能 <= cHat。如果cHat都比minCost大，那么这条分支就不可能取最优解。
				cHat, predictValidCost, predictValidOptimus := func() (float64, float64, []*simulator.Job) {
					// 这里重复利用下jobNamesInNode，避免重复劳动。
					jobNamesInNode[newJob.JobName()] = true
					// defer 千万别忘了将该jobName从该set中删除。
					defer delete(jobNamesInNode, newJob.JobName())
					// 找出剩余的不在该扩展后节点的job，组成otherJobs
					otherJobs := make([]*simulator.Job, 0, len(copiedJobs)-len(jobNamesInNode))
					otherJobsMap := make(map[simulator.JobName]*simulator.Job)
					for _, otherJob := range copiedJobs {
						if _, ok := jobNamesInNode[otherJob.JobName()]; !ok {
							otherJobs = append(otherJobs, otherJob)
							otherJobsMap[otherJob.JobName()] = otherJob
						}
					}

					// 将他们按照SRTF排序。
					jobs_util.GetJobsSliceUtil().ReorderToSRTF(gpu.Type(), otherJobs)
					// 构建新预测的完整jobs队列。这个队列的前面是当前已经扩展的部分节点包含的jobs
					// 后半部分是还未加入到当前解中的，其他的jobs。这些jobs按照SRTF排序了。
					predictJobList := make([]*simulator.Job, len(newJobs))
					copy(predictJobList, newJobs)
					predictJobList = append(predictJobList, otherJobs...)
					predictCostResp := costSolver.Cost(gpu, predictJobList)

					cHat := func() float64 {
						// 目前cHat的计算方法：将剩余任务按照SRTF排序后，计算cost，但是不考虑ddl违反带来的cost。
						// 这里可以直接利用predictJobList，因为他们的排序方案是一样的。
						// 这个jctCost是当前序列最想达到的
						cHat := predictCostResp.jctCost
						return cHat
					}()

					// 如果otherJobs这部分任务没有ddl违反，则该predict的job list就是当前分支的最优解
					otherJobsContainsViolated := func() bool {
						for _, violatedJob := range predictCostResp.ddlViolatedJobs {
							if _, ok := otherJobsMap[violatedJob.JobName()]; ok {
								return true
							}
						}
						return false
					}()
					if otherJobsContainsViolated {
						return cHat, predictCostResp.cost, predictJobList
					}

					return cHat, predictCostResp.cost, nil
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
					cost:        costResp.cost,
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
}

// NewMinCostByBacktrace 通过回溯计算MinCost。TODO
func NewMinCostByBacktrace() MinCostAlgo {
	return func(costSolver CostSolver, gpu *simulator.GPU, jobs []*simulator.Job) (float64, []*simulator.Job) {
		panic("Implement Me")
	}
}
