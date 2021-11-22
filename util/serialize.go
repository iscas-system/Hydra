package util

import "github.com/kr/pretty"

type PrettyExpose interface {
	PrettyExpose() interface{}
}

func Pretty(e interface{}) string {
	return pretty.Sprint(expose(e))
}

func expose(v interface{}) interface{} {
	switch v := v.(type) {
	case PrettyExpose:
		return v.PrettyExpose()
	default:
		return v
	}
}

func PrettyF(format string, vs ...interface{}) string {
	r := make([]interface{}, 0, len(vs))
	for _, v := range vs {
		r = append(r, expose(v))
	}
	return pretty.Sprintf(format, r...)
}
