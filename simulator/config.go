package simulator

import (
	"DES-go/schedulers/types"
	"os"
)

type LogPrintLevel int

const (
	NoPrint        = LogPrintLevel(0)
	ShortMsgPrint  = LogPrintLevel(1)
	AllFormatPrint = LogPrintLevel(2)
)

type Options struct {
	logEnabled              bool
	logDirPath              string
	gpuType2Count           map[types.GPUType]int
	minDurationPassInterval types.Duration
	dataSourceCSVPath       string
	dataSourceRange         []int
	formatPrintLevel        LogPrintLevel
}

var defaultOptions = &Options{
	logEnabled: true,
	logDirPath: os.TempDir(),
	gpuType2Count: map[types.GPUType]int{
		"V100": 1,
		"A100":   1,
		"GTX2080Ti": 1,
	},
	minDurationPassInterval: 1.,
	dataSourceCSVPath:       "",
	dataSourceRange:         nil,
	formatPrintLevel:        ShortMsgPrint,
}

type SetOption func(options *Options)

//func WithOptionLogEnabled(enabled bool) SetOption {
//	return func(options *Options) {
//		options.logEnabled = enabled
//	}
//}

func WithOptionLogPath(logPath string) SetOption {
	return func(options *Options) {
		options.logDirPath = logPath
	}
}

func WithOptionGPUType2Count(gpuType2Count map[string]int) SetOption {
	return func(options *Options) {
		transformed := make(map[types.GPUType]int)
		for gpuTypeStr, c := range gpuType2Count {
			transformed[types.GPUType(gpuTypeStr)] = c
		}
		options.gpuType2Count = transformed
	}
}

func WithOptionDataSourceCSVPath(csvPath string) SetOption {
	return func(options *Options) {
		options.dataSourceCSVPath = csvPath
	}
}

func WithOptionDataSourceRange(start, end int) SetOption {
	return func(options *Options) {
		options.dataSourceRange = []int{start, end}
	}
}

func WithOptionLogPrintLevel(logLevel LogPrintLevel) SetOption {
	return func(options *Options) {
		options.formatPrintLevel = logLevel
	}
}

func WithOptionMinDurationPassInterval(minDurationPassInterval types.Duration) SetOption {
	return func(options *Options) {
		options.minDurationPassInterval = minDurationPassInterval
	}
}
