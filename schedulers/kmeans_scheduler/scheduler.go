package kmeans_scheduler

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/simulator"
	"DES-go/util"
	"container/heap"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Scheduler 大体思路：
// 通过类似KMeans聚类的方式，将所有待调度的job分片到每个可用资源上。
// ------------------------------------------------------------------------------------------------------------------------
// 版本1 思路如下：
// 考虑将所有job和GPU放在一个平面进行考虑，将job和GPU都作为点来考虑。
// 初始，将所有的GPU看做KMeans算法中的初始簇中心点。即，有多少个GPU，则K为多大。
// KMeans聚类的目标，即为，将所有的job划分到所有的GPU簇中。
// 考虑每个job到任意一个GPU的距离为，它放入到这个GPU簇之后（最优放置），对整个集群的JCT和DDL带来的伤害（用这种方式可以将soft DDL考虑进来）。
// 则，该伤害越小，距离越近。在每次迭代中，找出那个job对整个集群的伤害最小的。将它放置到簇中。
// 迭代完成后，就得到了针对每个GPU的，一个job的划分。
// ------------------------------------------------------------------------------------------------------------------------
type Scheduler struct {
	gpuCluster *simulator.Cluster

	// waitingJobs 按照任意一个GPU上的速度排序，它的顺序决定了进行算法时的迭代（KMeans聚类时算法）的顺序。（这个顺序会造成怎样的影响，有待商榷）
	waitingJobs []*simulator.Job

	opts *Options
}

// Options
// 选项模式。动态指定调度器的参数。
type Options struct {
	Scheme           Scheme
	DistanceAlgoArgs AlgoArgs
	DDLCostType      DDLCostType
}

// 指定默认的选项。
var defaultOptions = &Options{
	Scheme: &SimpleOneShotScheme{
		Preemptive:      false,
		PreemptiveCycle: -1,
	},
	DistanceAlgoArgs: &AlgoBranchAndBoundArgs{
		LCStandard: BranchAndBoundLCStandardPartialCost,
	},
	DDLCostType: DDLCostTypeStrict,
}

// SetOptions 选项模式的设值用函数。
type SetOptions func(options *Options)

// WithScheme 指定调度机制。
func WithScheme(scheme Scheme) SetOptions {
	return func(options *Options) {
		options.Scheme = scheme
	}
}

// WithDistanceAlgoArgs 指定KMeans用于测定距离的算法参数
func WithDistanceAlgoArgs(algoArgs AlgoArgs) SetOptions {
	return func(options *Options) {
		options.DistanceAlgoArgs = algoArgs
	}
}

// WithDDLCostType 定义DDL的cost类型。目前有strict和soft两种。如果后续需要增加参数，需要将他们从enum改为struct args。
func WithDDLCostType(ddlCostType DDLCostType) SetOptions {
	return func(options *Options) {
		options.DDLCostType = ddlCostType
	}
}

// Scheme 定义kMeans算法的上层策略，与如何确定距离无关。它实际上指定了一种特定的调度算法。
type Scheme interface {
	GetKMeansSchemeName() string
}

// SimpleOneShotScheme 目前实现的最简单的一种调度方法，即，将全部job一次性地使用KMeans策略，进行聚类。
type SimpleOneShotScheme struct {
	// 指定是否可抢占，如果在实际中是可抢占的，那么需要指定一个调度的周期（否则会造成每次调度时造成大量的任务启停开销）
	// 如果不指定周期，则为一个理想化的调度（现实中无法实现）
	Preemptive      bool
	PreemptiveCycle simulator.Duration
}

func (s *SimpleOneShotScheme) GetKMeansSchemeName() string {
	return "SimpleOneShotScheme"
}

// New 初始化该调度器。可指定选项参数。
func New(setOptions ...SetOptions) *Scheduler {
	opts := &Options{
		Scheme:           defaultOptions.Scheme,
		DistanceAlgoArgs: defaultOptions.DistanceAlgoArgs,
	}
	for _, setOption := range setOptions {
		setOption(opts)
	}
	return &Scheduler{
		opts: opts,
	}
}

// DoSchedule 调度入口
func (k *Scheduler) DoSchedule() {
	switch scheme := k.opts.Scheme.(type) {
	case *SimpleOneShotScheme:
		k.doSimpleOneShotSchedule(scheme)
	default:
		panic("Unsupported scheme")
	}
}

func (k *Scheduler) SetCluster(cluster *simulator.Cluster) {
	k.gpuCluster = cluster
	k.waitingJobs = make([]*simulator.Job, 0, 1)
}

func (k *Scheduler) insertJobs2Waiting(jobs ...*simulator.Job) {
	// 这里将jobs插入到waitingJobs当中
	// 在这里指定了waitingJobs的排序顺序，也就决定了将来指定层次聚类算法的迭代顺序。
	// 使用随意选择的GPU，按照任务在它上面执行的剩余时间进行排序。（替代方案，可以选用最快的GPU进行排序，毕竟某些任务在慢的GPU上可能差距很小）
	sortedByGPUType := k.gpuCluster.GPUTypes()[0]
	for _, job := range jobs {
		target := job.RemainingDuration(sortedByGPUType)
		i := sort.Search(len(k.waitingJobs), func(i int) bool {
			return k.waitingJobs[i].RemainingDuration(sortedByGPUType) >= target
		})
		k.waitingJobs = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, i, k.waitingJobs)
	}
}

func (k *Scheduler) OnScheduleEvent(event simulator.ScheduleEvent) {
	switch e := event.(type) {
	case *simulator.ScheduleEventJobsArrived:
		{
			newJobs := make([]*simulator.Job, 0, len(e.JobMetas()))
			for _, jobMeta := range e.JobMetas() {
				newJobs = append(newJobs, simulator.NewJob(jobMeta.JobName()))
			}
			k.insertJobs2Waiting(newJobs...)
			if len(k.gpuCluster.EmptyGPUJobQueues()) > 0 {
				k.DoSchedule()
			}
		}
	case *simulator.ScheduleEventDurationPassed:
		{
			// ignore
		}
	case *simulator.ScheduleEventJobsFinished:
		{
			k.DoSchedule()
		}
	}
}

func (k *Scheduler) NextActiveScheduleTime() simulator.Time {
	return simulator.Time(math.Inf(1))
}

func (k *Scheduler) Name() string {
	return "Scheduler"
}

// --- 接下来定义调度的Scheme ---------------------------------------------------------------------------------------------

// doSimpleOneShotSchedule 简单的one shot机制。将全部jobs一次性使用KMeans得出分配结果。
func (k *Scheduler) doSimpleOneShotSchedule(scheme *SimpleOneShotScheme) {
	if scheme.Preemptive {
		// TODO 抢占式暂时先不做。
	}
	// 如果需要做非抢占式的，那么意味着每个waiting的job到任意一个簇的距离，需要考虑到当前正在执行的任务的剩余时间。
	// 初始，先将每个GPU放到簇中。
	// 每个簇中的jobs使用slice存储，但是一般时刻不关心它的顺序。
	// 只有最优解的顺序是我们关心的。所以每次将最优解的job sequence赋值到簇中。
	distanceSolver := newJobDistanceSolver(k.gpuCluster, k.opts.DistanceAlgoArgs, k.opts.DDLCostType)
	kMeansCluster := make(map[*simulator.GPU][]*simulator.Job)
	for gpuID := range k.gpuCluster.GPUJobQueues() {
		gpu := k.gpuCluster.GPU(gpuID)
		kMeansCluster[gpu] = make([]*simulator.Job, 0)
	}
	for len(k.waitingJobs) > 0 {
		// 计算每个点到每个簇的距离，选择一个最小的。
		minDis := math.Inf(1)
		var bestJobsSeq []*simulator.Job = nil
		var bestGPU *simulator.GPU = nil
		var bestJobIdx int
		wg := &sync.WaitGroup{}
		mu := &sync.Mutex{}
		for idx, waitingJob := range k.waitingJobs {
			idx := idx
			waitingJob := waitingJob
			util.GoWithWG(wg, idx, func(idx int) {
				innerWg := &sync.WaitGroup{}
				for gpu, jobsInCluster := range kMeansCluster {
					gpu := gpu
					jobsInCluster := jobsInCluster
					idx := idx
					util.GoWithWG(innerWg, 0, func(_ int) {
						distanceResp := distanceSolver.Distance(gpu, jobsInCluster, waitingJob)
						mu.Lock()
						if distanceResp.distance < minDis {
							minDis = distanceResp.distance
							bestJobsSeq = distanceResp.jobsSeq
							bestGPU = gpu
							bestJobIdx = idx
						}
						mu.Unlock()
					})
				}
				innerWg.Wait()
			})
		}
		wg.Wait()
		kMeansCluster[bestGPU] = bestJobsSeq
		_, k.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(bestJobIdx, k.waitingJobs)
	}
	for gpuID, queue := range k.gpuCluster.GPUJobQueues() {
		gpu := k.gpuCluster.GPU(gpuID)
		// 找到空闲的队列。
		if len(queue.Jobs()) == 0 {
			if len(kMeansCluster[gpu]) > 0 {
				// 将该GPU簇对应的最优序列的第一个任务放置到空闲位置上。
				var removed *simulator.Job
				removed, kMeansCluster[gpu] = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(0, kMeansCluster[gpu])
				queue.SetJobs([]*simulator.Job{removed})
			}
		}
	}
	// 将剩余的没有调度到GPU上的job重新放置到waitingJobs列表中。
	for _, jobs := range kMeansCluster {
		k.insertJobs2Waiting(jobs...)
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// --- 以下部分定义了算法的核心内容，即如何确定一个簇和一个点之间的距离。

type AlgoType int
type DDLCostType int

type AlgoArgs interface {
	GetType() AlgoType
}

const (
	AlgoTypeBranchAndBound        AlgoType = 0 // 分支限界法，求最优解。在选取活节点时使用部分jobs的成本排序。
	AlgoTypeSimpleHeuristicGreedy AlgoType = 2 // 贪心算法，求快速近似解。

	DDLCostTypeStrict DDLCostType = 0 // Strict，表示严格的DDL要求，即只要违约了DDL一点点，就认为非常严重。
	DDLCostTypeSoft   DDLCostType = 1 // Soft，表示较为宽松的DDL要求。

	ddlStrictCostCoefficient = float64(1e20)
)

// jobDistanceSolver
// 本结构用来计算一批任务在某个价值函数下的放置顺序。
// 可以实现为包含多种算法，比如最优的放置可以使用分支限界搜索方法。
// 如果速度较慢可以使用启发式的贪心算法。
type jobDistanceSolver struct {
	gpuCluster  *simulator.Cluster
	algoArgs    AlgoArgs
	ddlCostType DDLCostType

	// 避免重复计算，使用memo记录重复参数的调用。
	distanceMemo *sync.Map // map[string]*distanceResp
	costMemo     *sync.Map // map[string]*costResp
}

func newJobDistanceSolver(
	cluster *simulator.Cluster,
	algoArgs AlgoArgs,
	ddlCostType DDLCostType) *jobDistanceSolver {
	return &jobDistanceSolver{
		gpuCluster:   cluster,
		algoArgs:     algoArgs,
		ddlCostType:  ddlCostType,
		costMemo:     &sync.Map{}, // make(map[string]*costResp),
		distanceMemo: &sync.Map{}, // make(map[string]*distanceResp),
	}
}

// distanceMemoKey 为Distance调用生成一个key。用于区分相同的调用。
func (s *jobDistanceSolver) distanceMemoKey(kMeansCenterGPU *simulator.GPU, kMeansPointJobs []*simulator.Job, jobNotInKMeansCluster *simulator.Job) string {
	// 计算距离时，不关心已经在簇里的jobs的顺序，所以需要先按照固定顺序排序。
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(kMeansCenterGPU.Type(), kMeansPointJobs)
	builder := &strings.Builder{}
	// gpu info
	builder.WriteString("GPU:")
	builder.WriteString(kMeansCenterGPU.String())
	writeJob := func(job *simulator.Job) {
		builder.WriteString(string(job.JobName()))
		builder.WriteString(strconv.FormatFloat(job.RemainingRatio(), 'f', 6, 64))
		builder.WriteByte('-')
	}
	if runningJob := s.gpuCluster.CurrRunningJob(kMeansCenterGPU.ID()); runningJob != nil {
		builder.WriteString("Running:")
		writeJob(runningJob)
	}
	builder.WriteString("InCluster:")
	for _, job := range kMeansPointJobs {
		writeJob(job)
	}
	builder.WriteString("jobNotInKMeansCluster:")
	writeJob(jobNotInKMeansCluster)
	return builder.String()
}

// costMemoKey 为cost调用生成一个key。用于区分相同的调用。
func (s *jobDistanceSolver) costMemoKey(gpu *simulator.GPU, jobs []*simulator.Job) string {
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

type distanceResp struct {
	jobsSeq  []*simulator.Job
	distance float64
}

type costResp struct {
	cost        float64
	jctCost     float64
	ddlCost     float64
	ddlViolated bool
}

// Distance 定义算法入口
// Scheduler 通过使用该方法，获得某个job到某簇的距离
func (s *jobDistanceSolver) Distance(kMeansCenterGPU *simulator.GPU, kMeansPointJobs []*simulator.Job, jobNotInKMeansCluster *simulator.Job) *distanceResp {
	copiedSlice := jobs_util.GetJobsSliceUtil().Copy(kMeansPointJobs)
	memoKey := s.distanceMemoKey(kMeansCenterGPU, copiedSlice, jobNotInKMeansCluster)
	if memorized, ok := s.distanceMemo.Load(memoKey); ok {
		return memorized.(*distanceResp)
	}
	var distanceResp *distanceResp = nil
	switch args := s.algoArgs.(type) {
	case *AlgoBranchAndBoundArgs:
		distanceResp = s.distanceBranchAndBound(kMeansCenterGPU, copiedSlice, jobNotInKMeansCluster, args)
	case *AlgoSimpleHeuristicGreedyArgs:
		distanceResp = s.distanceSimpleHeuristicGreedy(kMeansCenterGPU, copiedSlice, jobNotInKMeansCluster, args)
	default:
		panic("Unsupported algoType")
	}
	s.distanceMemo.Store(memoKey, distanceResp)
	return distanceResp
}

// cost 本函数为该算法的核心部分，它定义在一个gpu上的一组排序号的jobs，它的总代价是多大。
// 目前将代价分为两部分，一部分是JCT，另一部分是DDL违约。
// 那么JCT就按照累加求和即可，而DDL作为更为首要的要求，可以使用一个高倍的系数，乘以每个违约job的违约时长，使得它比JCT更重要。
// 那么这里也可以加入soft DDL的模式，即当job只违反了一点点DDL时，不认为它非常严重。
// 返回值: 分别返回，代价的大小（float64），以及是否存在DDL违反（bool）。
func (s *jobDistanceSolver) cost(gpu *simulator.GPU, jobs []*simulator.Job) *costResp {
	// 如果有过相同调用，则直接返回记录的结果。
	memoKey := s.costMemoKey(gpu, jobs)
	if memorized, ok := s.costMemo.Load(memoKey); ok {
		return memorized.(*costResp)
	}

	jctOffset := s.gpuCluster.Now()
	// 考虑到非抢占式调度，要将当前正在运行的任务剩余运行时间考虑进来。
	runningJob := s.gpuCluster.CurrRunningJob(gpu.ID())
	if runningJob != nil {
		jctOffset += simulator.Time(runningJob.RemainingDuration(gpu.Type()))
	}

	// 第一步，计算每个任务的jct，以及每个任务违反ddl的时长。
	JCTs, ddlViolations := func() ([]simulator.Time, []simulator.Duration) {
		JCTs := make([]simulator.Time, 0, len(jobs))
		ddlViolations := make([]simulator.Duration, 0, len(jobs))
		prevSumJCT := jctOffset
		for _, job := range jobs {
			// 此处是预测job的JCT，不是计算已经完成的任务的JCT，所以不可以调用job.JCT()，因为job.JCT()只有当任务实际已经完成时才能返回结果。
			currJobJCT := prevSumJCT + simulator.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
			prevSumJCT += prevSumJCT + currJobJCT
			JCTs = append(JCTs, currJobJCT)
			if currJobJCT > job.JobMeta().DDL() {
				ddlViolations = append(ddlViolations, simulator.Duration(currJobJCT-job.JobMeta().DDL()))
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
			switch s.ddlCostType {
			case DDLCostTypeStrict:
				ddlViolationCosts = append(ddlViolationCosts, ddlStrictCostCoefficient*float64(violation))
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
		cost:        JCTCost + DDLCost,
		jctCost:     JCTCost,
		ddlCost:     DDLCost,
		ddlViolated: DDLCost > 0,
	}
	s.costMemo.Store(memoKey, costResp)
	return costResp
}

// ------------------------------------------------ 分支限界法 -----------------------------------------------------------

type AlgoBranchAndBoundArgs struct {
	LCStandard BranchAndBoundLCStandard
}

func (k *AlgoBranchAndBoundArgs) GetType() AlgoType {
	return AlgoTypeBranchAndBound
}

type BranchAndBoundLCStandard int

const (
	BranchAndBoundLCStandardPartialCost BranchAndBoundLCStandard = 0
	BranchAndBoundLCStandardPredictCost BranchAndBoundLCStandard = 1
)

// distanceBranchAndBound
// 首先，将任务经过SJF排序，如果没有任何job违反ddl，则这个放置就会是最优的，不需要经过后续过程。
// 如果，有任务违反了ddl，则进行分支限界法搜索。
// 使用MinCost顺序选择开节点，每个节点的估计成本为：将所有未放置到搜索路径的任务按照SJF排序后，该队列的代价值。
func (s *jobDistanceSolver) distanceBranchAndBound(
	kMeansCenterGPU *simulator.GPU,
	kMeansPointJobs []*simulator.Job,
	jobNotInKMeansCluster *simulator.Job,
	args *AlgoBranchAndBoundArgs) *distanceResp {

	// 不关心一个簇中任务的顺序。
	jobs := append(kMeansPointJobs, jobNotInKMeansCluster)
	// 首先尝试将jobs使用SRTF排序，并计算一次cost。如果发现ddl没有被违反，则使用这个排序即可。
	//（实际上，算法之所以在总体上不是最优的（由于NP-hard，且不知道任务的到来，所以算不出最优），
	// 也是由于在不违反ddl时，只能使用SJF去思考，无法预测将来的任务到来是否会打散当前的SJF排序。
	// 这是一种贪心的思想。不过只要无法预测将来任务的到来，就不可能做出最优解。）
	// 不过是否可以再用一个度量指标，用于描述这个job有多么容易违反ddl？（离违反ddl有多近）这可以作为之后的改进思路。
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(kMeansCenterGPU.Type(), jobs)
	costResp := s.cost(kMeansCenterGPU, jobs)
	if !costResp.ddlViolated {
		return &distanceResp{
			jobsSeq:  jobs,
			distance: costResp.cost,
		}
	}
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
	}
	nodes := make([]*Node, 0, 1)
	// 初始节点，没有任何元素的一个节点。
	nodes = append(nodes, &Node{
		[]*simulator.Job{}, math.Inf(1), math.Inf(1),
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
			switch args.LCStandard {
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
	for minHeap.Len() > 0 {
		expandingNode := heap.Pop(minHeap).(*Node)
		// 首先查看当前节点是否是一个答案节点。
		if len(expandingNode.jobs) == len(jobs) {
			// 如果是，则更新minCost和optimus
			if expandingNode.cost <= minCost {
				minCost = expandingNode.cost
				optimus = expandingNode.jobs
			}
			continue
		}
		// 如果是非答案节点，则首先建立一个minCostNode当前包含的job的集合，
		// 用于查询当前有哪些任务还没在这个队列里。
		jobNamesInNode := make(map[simulator.JobName]bool)
		for _, job := range expandingNode.jobs {
			jobNamesInNode[job.JobName()] = true
		}
		// 遍历全部job的队列，找到那些不在当前Node包含的jobs中的那些job，尝试对它们进行扩展生成新的活节点。
		for _, job := range jobs {
			if _, ok := jobNamesInNode[job.JobName()]; ok {
				continue
			}
			// 找到了，则尝试对它扩展。
			newJob := job
			newJobs := make([]*simulator.Job, len(expandingNode.jobs))
			copy(newJobs, expandingNode.jobs)
			newJobs = append(newJobs, newJob)
			costResp := s.cost(kMeansCenterGPU, newJobs)
			if costResp.cost > minCost {
				// 当前不完全的jobs队列的cost已经大于minCost了，
				// 那么它在后续继续添加任务，cost只可能增加。所以剪枝。
				continue
			}
			// 对当前的扩展节点计算一个预测的cost，将其他未在队列的jobs都加入进来，并按照SJF排序，计算一个cost。它必定>=最优解的cost。
			// 如果预测的cost小于minCost，则将minCost更新为predictCost，这样能够任意一个答案节点计算出来之前，获得一个cost上界。
			predictCost := func() float64 {
				// 这里重复利用下jobNamesInNode，避免重复劳动。
				jobNamesInNode[newJob.JobName()] = true
				// defer 千万别忘了将该jobName从该set中删除。
				defer delete(jobNamesInNode, newJob.JobName())
				// 找出剩余的不在该扩展后节点的job，组成otherJobs
				otherJobs := make([]*simulator.Job, 0, len(jobs)-len(jobNamesInNode))
				for _, otherJob := range jobs {
					if _, ok := jobNamesInNode[otherJob.JobName()]; !ok {
						otherJobs = append(otherJobs, otherJob)
					}
				}
				// 将他们按照SRTF排序。
				jobs_util.GetJobsSliceUtil().ReorderToSRTF(kMeansCenterGPU.Type(), otherJobs)
				// 构建新的预测的完整jobs队列。
				predictJobList := make([]*simulator.Job, len(newJobs))
				copy(predictJobList, newJobs)
				predictJobList = append(predictJobList, otherJobs...)
				predictCostResp := s.cost(kMeansCenterGPU, predictJobList)
				return predictCostResp.cost
			}()

			// 尝试更新minCost
			minCost = math.Min(minCost, predictCost)

			newNode := &Node{
				jobs:        newJobs,
				cost:        costResp.cost,
				predictCost: predictCost,
			}
			// 将扩展好的节点加入到最小堆。
			heap.Push(minHeap, newNode)
		}
	}

	// 当minHeap为空时，分支限界法结束。
	return &distanceResp{
		jobsSeq:  optimus,
		distance: minCost,
	}
}

// ------------------------------------------------ 贪心算法 ------------------------------------------------------------

type AlgoSimpleHeuristicGreedyArgs struct {
}

func (k *AlgoSimpleHeuristicGreedyArgs) GetType() AlgoType {
	return AlgoTypeSimpleHeuristicGreedy
}

func (s *jobDistanceSolver) distanceSimpleHeuristicGreedy(
	kMeansCenterGPU *simulator.GPU,
	kMeansPointJobs []*simulator.Job,
	jobNotInKMeansCluster *simulator.Job,
	args *AlgoSimpleHeuristicGreedyArgs) *distanceResp {
	// TODO
	return nil

}
