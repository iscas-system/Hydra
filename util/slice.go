package util

func StringSliceIndexOf(slice []string, target string) int {
	for i, s := range slice {
		if s == target {
			return i
		}
	}
	return -1
}

func SumFloat64(f func(i interface{}) float64, vs ...interface{}) float64 {
	s := 0.
	for _, v := range vs {
		s += f(v)
	}
	return s
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
