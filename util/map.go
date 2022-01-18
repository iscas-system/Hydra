package util

func CopyStringIntMap(o map[string]int) map[string]int {
	r := make(map[string]int)
	for k, v := range o {
		r[k] = v
	}
	return r
}

func StringIntMapLessOrEqualsKeys(o map[string]int, target map[string]int) []string {
	keys := make([]string, 0)
	for k := range o {
		if o[k] < target[k] {
			keys = append(keys, k)
		}
	}
	return keys
}
