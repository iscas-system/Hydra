package simulator

import "os"

type Options struct {
	logEnabled              bool
	logDirPath              string
	gpuType2Count           map[GPUType]int
	minDurationPassInterval Duration
}

var defaultOptions *Options = &Options{
	logEnabled: true,
	logDirPath: os.TempDir(),
	gpuType2Count: map[GPUType]int{
		"V100": 1,
		"T4":   1,
		"P100": 1,
	},
	minDurationPassInterval: 1.,
}

type SetOption func(options *Options)

func WithOptionLogEnabled(enabled bool) SetOption {
	return func(options *Options) {
		options.logEnabled = enabled
	}
}

func WithOptionLogPath(logPath string) SetOption {
	return func(options *Options) {
		options.logDirPath = logPath
	}
}

func WithOptionGPUType2Count(gpuType2Count map[GPUType]int) SetOption {
	return func(options *Options) {
		options.gpuType2Count = gpuType2Count
	}
}

func WithOptionMinDurationPassInterval(minDurationPassInterval Duration) SetOption {
	return func(options *Options) {
		options.minDurationPassInterval = minDurationPassInterval
	}
}
