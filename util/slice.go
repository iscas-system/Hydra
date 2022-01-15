package util

import "math"

func StringSliceIndexOf(slice []string, target string) int {
	for i, s := range slice {
		if s == target {
			return i
		}
	}
	return -1
}

func SumFloat64(f func(item interface{}) float64, vs ...interface{}) float64 {
	s := 0.
	for _, v := range vs {
		s += f(v)
	}
	return s
}

func SumInt64(f func(item interface{}) int64, vs ...interface{}) int64 {
	s := int64(0)
	for _, v := range vs {
		s += f(v)
	}
	return s
}

func MaxInt64(f func(item interface{}) int64, vs ...interface{}) int64 {
	max := math.Inf(-1)
	for _, v := range vs {
		max = math.Max(max, float64(f(v)))
	}
	return int64(max)
}

func MinInt64(f func(item interface{}) int64, vs ...interface{}) int64 {
	min := math.Inf(1)
	for _, v := range vs {
		min = math.Min(min, float64(f(v)))
	}
	return int64(min)
}

func AvgFloat64(f func(i interface{}) float64, vs ...interface{}) float64 {
	return SumFloat64(f, vs...) / float64(len(vs))
}

func SliceInsert(idx int, v interface{}, ls ...interface{}) []interface{} {
	rear := append([]interface{}{}, ls[idx:]...)
	res := append(ls[:idx], v)
	res = append(res, rear...)
	return res
}
