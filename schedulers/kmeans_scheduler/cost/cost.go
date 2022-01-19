package cost

import (
	"DES-go/schedulers/types"
	"DES-go/util"
	"strconv"
	"strings"
	"sync"
)

// solverCommon 定义了成本计算的公用类型。
type solverCommon struct {
	memo            *sync.Map // map[string]*Resp
	jctOffsetGetter jctOffsetGetter
}

func newCostSolverCommon(defaultJCTOffsetGetter jctOffsetGetter) *solverCommon {
	return &solverCommon{
		memo:            &sync.Map{},
		jctOffsetGetter: defaultJCTOffsetGetter,
	}
}

type jctOffsetGetter func(gpu types.GPU) types.Time

func (c *solverCommon) costMemoKey(gpu types.GPU, jobs []types.Job, jctOffset types.Time) string {
	builder := &strings.Builder{}
	// gpu info
	builder.WriteString("GPU:")
	builder.WriteString(gpu.String())
	builder.WriteString("JCTOffset:")
	builder.WriteString(strconv.FormatFloat(float64(jctOffset), 'f', 6, 64))
	writeJob := func(job types.Job) {
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

func (c *solverCommon) CalJCTAndDDLViolations(jctOffset types.Time, gpu types.GPU, jobs []types.Job) ([]types.Time, []types.Duration, []types.Job) {
	JCTs := make([]types.Time, 0, len(jobs))
	ddlViolations := make([]types.Duration, 0, len(jobs))
	ddlViolatedJobs := make([]types.Job, 0)
	for _, job := range jobs {
		// 此处是预测job的JCT，不是计算已经完成的任务的JCT，所以不可以调用job.JCT()，因为job.JCT()只有当任务实际已经完成时才能返回结果。
		currJobJCT := jctOffset + types.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
		jctOffset = currJobJCT
		JCTs = append(JCTs, currJobJCT)
		if currJobJCT > job.JobMeta().DDL() {
			ddlViolations = append(ddlViolations, types.Duration(currJobJCT-job.JobMeta().DDL()))
			ddlViolatedJobs = append(ddlViolatedJobs, job)
		} else {
			ddlViolations = append(ddlViolations, 0)
		}
	}
	return JCTs, ddlViolations, ddlViolatedJobs
}

func (c *solverCommon) JCTOffset(gpu types.GPU) types.Time {
	return c.jctOffsetGetter(gpu)
}

// Solver 定义成本计算方式。
type Solver interface {
	Cost(gpu types.GPU, jobs []types.Job) *Resp
	JCTOffset(gpu types.GPU) types.Time
}

type SolverMaker func(jctOffsetGetter jctOffsetGetter) Solver

type DDLCostType int

const (
	DDLCostTypeStrict DDLCostType = 0 // Strict，表示严格的DDL要求，即只要违约了DDL一点点，就认为非常严重。
	DDLCostTypeSoft   DDLCostType = 1 // Soft，表示较为宽松的DDL要求。
)

type Resp struct {
	Cost            float64
	JCTCost         float64
	DDLCost         float64
	DDLViolatedJobs []types.Job
	DDLViolated     bool
}

// ---------------------------------------- SimpleAddSolver ---------------------------------------

// SimpleAddSolver 简单的将JCT与DDL violation相加作为cost。参数可以指定ddl系数等。
type SimpleAddSolver struct {
	*solverCommon
	ddlCostType              DDLCostType
	ddlStrictCostCoefficient float64
}

func NewSimpleAddCostSolverMaker(ddlCostType DDLCostType, ddlStrictCostCoefficient float64) SolverMaker {
	return func(defaultJCTOffsetGetter jctOffsetGetter) Solver {
		return &SimpleAddSolver{
			solverCommon:             newCostSolverCommon(defaultJCTOffsetGetter),
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
func (s *SimpleAddSolver) Cost(gpu types.GPU, jobs []types.Job) *Resp {
	// 如果有过相同调用，则直接返回记录的结果。
	jctOffset := s.jctOffsetGetter(gpu)
	memoKey := s.costMemoKey(gpu, jobs, jctOffset)
	if memorized, ok := s.memo.Load(memoKey); ok {
		return memorized.(*Resp)
	}

	// 第一步，计算每个任务的jct，以及每个任务违反ddl的时长。
	JCTs, ddlViolations, ddlViolatedJobs := s.CalJCTAndDDLViolations(jctOffset, gpu, jobs)

	// 第二步，计算jct带来的cost。
	JCTCost := func() float64 {
		// 目前，简单的将JCT求和后的值作为JCT Costs。这里在后面可以进行修改，比如增加一些系数。
		interfaceJCTs := make([]interface{}, len(JCTs))
		for idx, jct := range JCTs {
			interfaceJCTs[idx] = jct
		}
		return util.SumFloat64(func(item interface{}) float64 {
			return float64(item.(types.Time))
		}, interfaceJCTs...)
	}()

	// 第三步，计算DDL violation带来的cost。
	DDLCost := func() float64 {
		// 计算每个job的ddl violation Cost，并求和
		ddlViolationCosts := make([]interface{}, 0, len(ddlViolations))
		for _, violation := range ddlViolations {
			if violation <= 0 {
				continue
			}
			switch s.ddlCostType {
			case DDLCostTypeStrict:
				ddlViolationCosts = append(ddlViolationCosts, s.ddlStrictCostCoefficient*float64(violation))
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
	costResp := &Resp{
		Cost:            JCTCost + DDLCost,
		JCTCost:         JCTCost,
		DDLCost:         DDLCost,
		DDLViolated:     DDLCost > 0,
		DDLViolatedJobs: ddlViolatedJobs,
	}
	s.memo.Store(memoKey, costResp)
	return costResp
}

type MinCostParams struct {
	CostSolver Solver
	GPU        types.GPU
	Jobs       []types.Job
}

type MinCostAlgo interface {
	MinCost(params *MinCostParams) (float64, []types.Job)
	String() string
	RecordExtra() interface{}
}
