package util

import "time"

func makeDurationInterfaceSlice(vs ...time.Duration) []interface{} {
	ifs := make([]interface{}, len(vs))
	for idx, v := range vs {
		ifs[idx] = v
	}
	return ifs
}

func AvgDuration(vs ...time.Duration) time.Duration {
	if len(vs) == 0 {
		return 0
	}
	return time.Duration(SumInt64(func(item interface{}) int64 {
		return int64(item.(time.Duration))
	}, makeDurationInterfaceSlice(vs...)...) / int64(len(vs)))
}

func MaxDuration(vs ...time.Duration) time.Duration {
	return time.Duration(MaxInt64(func(item interface{}) int64 {
		return int64(item.(time.Duration))
	}, makeDurationInterfaceSlice(vs...)...))
}

func MinDuration(vs ...time.Duration) time.Duration {
	return time.Duration(MinInt64(func(item interface{}) int64 {
		return int64(item.(time.Duration))
	}, makeDurationInterfaceSlice(vs...)...))
}
