package kmeans_scheduler

import (
	"DES-go/schedulers/jobs_util"
	"DES-go/schedulers/types"
	"DES-go/util"
	"fmt"
	"log"
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
	gpuCluster types.Cluster
	// waitingJobs 按照任意一个GPU上的速度排序，它的顺序决定了进行算法时的迭代（KMeans聚类时算法）的顺序。（这个顺序会造成怎样的影响，有待商榷）
	waitingJobs []types.Job
	opts        *Options

	allArrivedJobsCount int
}

// Options
// 选项模式。动态指定调度器的参数。
type Options struct {
	ScheduleScheme ScheduleScheme
	DistanceAlgo   DistanceAlgo
}

// 指定默认的选项。
var defaultOptions = &Options{
	ScheduleScheme: NewSimpleOneShotScheduleScheme(false, -1),
	DistanceAlgo:   NewMinCostDistanceAlgo(NewMinCostByBranchAndBoundAlgo(MinCostBranchAndBoundLCStandardPartialCost), NewSimpleAddCostSolverMaker(DDLCostTypeStrict, 1e20)),
}

// SetOptions 选项模式的设值用函数。
type SetOptions func(options *Options)

// WithScheme 指定调度机制。
func WithScheme(scheduleScheme ScheduleScheme) SetOptions {
	return func(options *Options) {
		options.ScheduleScheme = scheduleScheme
	}
}

// WithDistanceAlgo 指定KMeans用于测定距离的算法
func WithDistanceAlgo(distanceAlgo DistanceAlgo) SetOptions {
	return func(options *Options) {
		options.DistanceAlgo = distanceAlgo
	}
}

// New 初始化该调度器。可指定选项参数。
func New(setOptions ...SetOptions) *Scheduler {
	opts := &Options{
		ScheduleScheme: defaultOptions.ScheduleScheme,
		DistanceAlgo:   defaultOptions.DistanceAlgo,
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
	k.opts.ScheduleScheme.DoSchedule(k)
}

func (k *Scheduler) SetCluster(cluster types.Cluster) {
	k.gpuCluster = cluster
	k.waitingJobs = make([]types.Job, 0, 1)
}

func (k *Scheduler) insertJobs2Waiting(jobs ...types.Job) {
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

func (k *Scheduler) OnScheduleEvent(event types.ScheduleEvent) {
	switch e := event.(type) {
	case *types.ScheduleEventJobsArrived:
		{
			k.allArrivedJobsCount += len(e.JobMetas())
			fmt.Printf("allArrivedJobsCount = [%d]\n", k.allArrivedJobsCount)
			newJobs := make([]types.Job, 0, len(e.JobMetas()))
			for _, jobMeta := range e.JobMetas() {
				newJobs = append(newJobs, k.gpuCluster.InitJob(jobMeta.JobName()))
			}
			k.insertJobs2Waiting(newJobs...)
			if len(k.gpuCluster.EmptyGPUJobQueues()) > 0 {
				k.DoSchedule()
			}
		}
	case *types.ScheduleEventDurationPassed:
		{
			// ignore
		}
	case *types.ScheduleEventJobsFinished:
		{
			k.DoSchedule()
		}
	}
}

func (k *Scheduler) NextActiveScheduleTime() types.Time {
	return types.Time(math.Inf(1))
}

func (k *Scheduler) Name() string {
	return fmt.Sprintf("KMeansScheduler[opts=%s]", util.Pretty(k.opts))
}

// --- 接下来定义调度的Scheme ---------------------------------------------------------------------------------------------

type ScheduleScheme interface {
	DoSchedule(scheduler *Scheduler)
}

type SimpleOneShotScheduleScheme struct {
	// 指定是否可抢占，如果在实际中是可抢占的，那么需要指定一个调度的周期（否则会造成每次调度时造成大量的任务启停开销）
	// 如果不指定周期，则为一个理想化的调度（现实中无法实现）
	Preemptive      bool
	PreemptiveCycle types.Duration

	ScheduleJobCount int
}

func NewSimpleOneShotScheduleScheme(Preemptive bool, PreemptiveCycle types.Duration) ScheduleScheme {
	return &SimpleOneShotScheduleScheme{
		Preemptive:       Preemptive,
		PreemptiveCycle:  PreemptiveCycle,
		ScheduleJobCount: 0,
	}
}

// DoSchedule 简单的one shot机制。将全部jobs一次性使用KMeans得出分配结果。
func (s *SimpleOneShotScheduleScheme) DoSchedule(scheduler *Scheduler) {
	if s.Preemptive {
		// TODO 抢占式暂时先不做。
	}
	// 如果需要做非抢占式的，那么意味着每个waiting的job到任意一个簇的距离，需要考虑到当前正在执行的任务的剩余时间。
	// 初始，先将每个GPU放到簇中。
	// 每个簇中的jobs使用slice存储，但是一般时刻不关心它的顺序。
	// 只有最优解的顺序是我们关心的。所以每次将最优解的job sequence赋值到簇中。
	distanceSolver := newJobDistanceSolver(scheduler.gpuCluster, scheduler.opts.DistanceAlgo)
	kMeansCluster := make(map[types.GPU][]types.Job)
	for gpuID := range scheduler.gpuCluster.GPUJobQueues() {
		gpu := scheduler.gpuCluster.GPU(gpuID)
		kMeansCluster[gpu] = make([]types.Job, 0)
	}
	for len(scheduler.waitingJobs) > 0 {
		// 计算每个点到每个簇的距离，选择一个最小的。
		minDis := math.Inf(1)
		var bestJobsSeq []types.Job = nil
		var bestGPU types.GPU = nil
		var bestJobIdx int
		wg := &sync.WaitGroup{}
		mu := &sync.Mutex{}
		for idx, waitingJob := range scheduler.waitingJobs {
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
						if len(distanceResp.jobsSeq) != (len(jobsInCluster) + 1) {
							panic("len(distanceResp.jobsSeq) != (len(jobsInCluster) + 1)")
						}
						mu.Lock()
						defer mu.Unlock()
						if distanceResp.distance < minDis {
							minDis = distanceResp.distance
							bestJobsSeq = distanceResp.jobsSeq
							bestGPU = gpu
							bestJobIdx = idx
						}
					})
				}
				innerWg.Wait()
			})
		}
		wg.Wait()
		kMeansCluster[bestGPU] = bestJobsSeq
		_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(bestJobIdx, scheduler.waitingJobs)
	}
	for gpuID, queue := range scheduler.gpuCluster.GPUJobQueues() {
		gpu := scheduler.gpuCluster.GPU(gpuID)
		// 找到空闲的队列。
		if len(queue.Jobs()) == 0 {
			if len(kMeansCluster[gpu]) > 0 {
				// 将该GPU簇对应的最优序列的第一个任务放置到空闲位置上。
				var removed types.Job
				removed, kMeansCluster[gpu] = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(0, kMeansCluster[gpu])
				queue.SetJobs(removed)
				s.ScheduleJobCount += 1
			}
		}
	}
	// 将剩余的没有调度到GPU上的job重新放置到waitingJobs列表中。
	for _, jobs := range kMeansCluster {
		scheduler.insertJobs2Waiting(jobs...)
	}
	// fmt.Printf("curr ScheduleJobCount = [%d]\n", s.ScheduleJobCount)
	log.Printf("curr ScheduleJobCount = [%d]\n", s.ScheduleJobCount)
}

// ---------------------------------------------------------------------------------------------------------------------
// --- 以下部分定义了算法的核心内容，即如何确定一个簇和一个点之间的距离。

// jobDistanceSolver
// 本结构用来计算一批任务在某个价值函数下的放置顺序。
// 可以实现为包含多种算法，比如最优的放置可以使用分支限界搜索方法。
// 如果速度较慢可以使用启发式的贪心算法。
type jobDistanceSolver struct {
	gpuCluster types.Cluster
	// 避免重复计算，使用memo记录重复参数的调用。
	distanceMemo *sync.Map // map[string]*distanceResp
	distanceAlgo DistanceAlgo
}

type DistanceAlgo interface {
	Distance(gpuCluster types.Cluster,
		kMeansCenterGPU types.GPU,
		kMeansPointJobs []types.Job,
		jobNotInKMeansCluster types.Job) *distanceResp
}

func newJobDistanceSolver(
	cluster types.Cluster,
	algo DistanceAlgo) *jobDistanceSolver {
	return &jobDistanceSolver{
		gpuCluster:   cluster,
		distanceMemo: &sync.Map{}, // make(map[string]*distanceResp),
		distanceAlgo: algo,
	}
}

// distanceMemoKey 为Distance调用生成一个key。用于区分相同的调用。
func (s *jobDistanceSolver) distanceMemoKey(kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job) string {
	// 计算距离时，不关心已经在簇里的jobs的顺序，所以需要先按照固定顺序排序。
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(kMeansCenterGPU.Type(), kMeansPointJobs)
	builder := &strings.Builder{}
	// gpu info
	builder.WriteString("GPU:")
	builder.WriteString(kMeansCenterGPU.String())
	writeJob := func(job types.Job) {
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

type distanceResp struct {
	jobsSeq  []types.Job
	distance float64
}

// Distance 定义算法入口
// Scheduler 通过使用该方法，获得某个job到某簇的距离
func (s *jobDistanceSolver) Distance(kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job) *distanceResp {
	copiedSlice := jobs_util.GetJobsSliceUtil().Copy(kMeansPointJobs)
	memoKey := s.distanceMemoKey(kMeansCenterGPU, copiedSlice, jobNotInKMeansCluster)
	if memorized, ok := s.distanceMemo.Load(memoKey); ok {
		return memorized.(*distanceResp)
	}
	distanceResp := s.distanceAlgo.Distance(s.gpuCluster, kMeansCenterGPU, copiedSlice, jobNotInKMeansCluster)
	s.distanceMemo.Store(memoKey, distanceResp)
	return distanceResp
}

// ------------------------------------------------ Distance具体算法 ----------------------------------------------------

type MinCostDistanceAlgo struct {
	minCostAlgo     MinCostAlgo
	costSolverMaker CostSolverMaker
}

// Distance 使用最小cost作为距离的算法参数
// 使用将新的任务放置到该簇后的minCost作为距离。
// 首先，将任务经过SJF排序，如果没有任何job违反ddl，则这个放置的cost就会是最优的，不需要经过后续过程。
// 如果，有任务违反了ddl，则进行分支限界法搜索。
// 使用MinCost顺序选择开节点，每个节点的估计成本为：将所有未放置到搜索路径的任务按照SJF排序后，该队列的代价值。
func (m *MinCostDistanceAlgo) Distance(gpuCluster types.Cluster, kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job) *distanceResp {
	// 不关心一个簇中任务的顺序。
	jobs := append(kMeansPointJobs, jobNotInKMeansCluster)
	// 首先尝试将jobs使用SRTF排序，并计算一次cost。如果发现ddl没有被违反，则使用这个排序即可。
	//（实际上，算法之所以在总体上不是最优的（由于NP-hard，且不知道任务的到来，所以算不出最优），
	// 也是由于在不违反ddl时，只能使用SJF去思考，无法预测将来的任务到来是否会打散当前的SJF排序。
	// 这是一种贪心的思想。不过只要无法预测将来任务的到来，就不可能做出最优解。）
	// 不过是否可以再用一个度量指标，用于描述这个job有多么容易违反ddl？（离违反ddl有多近）这可以作为之后的改进思路。
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(kMeansCenterGPU.Type(), jobs)
	costSolver := m.costSolverMaker(func(gpu types.GPU) types.Time {
		jctOffset := gpuCluster.Now()
		// 考虑到非抢占式调度，要将当前正在运行的任务剩余运行时间考虑进来。
		runningJob := gpuCluster.CurrRunningJob(gpu.ID())
		if runningJob != nil {
			jctOffset += types.Time(runningJob.RemainingDuration(gpu.Type()))
		}
		return jctOffset
	})
	costResp := costSolver.Cost(kMeansCenterGPU, jobs)
	if !costResp.ddlViolated {
		return &distanceResp{
			jobsSeq:  jobs,
			distance: costResp.cost,
		}
	}
	minCost, optimus := m.minCostAlgo.MinCost(costSolver, kMeansCenterGPU, jobs)
	return &distanceResp{
		jobsSeq:  optimus,
		distance: minCost,
	}
}

func NewMinCostDistanceAlgo(minCostAlgo MinCostAlgo, costSolverMaker CostSolverMaker) DistanceAlgo {
	return &MinCostDistanceAlgo{
		minCostAlgo:     minCostAlgo,
		costSolverMaker: costSolverMaker,
	}
}

// ------------------------------------------------ 贪心算法 ------------------------------------------------------------

type SimpleHeuristicGreedyDistanceAlgo struct {
}

func (s *SimpleHeuristicGreedyDistanceAlgo) Distance(gpuCluster types.Cluster, kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job) *distanceResp {
	panic("Implement Me.")
}
