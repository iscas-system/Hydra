package simulator

import "os"

type FormatPrintLevel int

const (
	NoFormatPrint  = FormatPrintLevel(0)
	ShortMsgPrint  = FormatPrintLevel(1)
	AllFormatPrint = FormatPrintLevel(2)
)

type Options struct {
	logEnabled              bool
	logDirPath              string
	gpuType2Count           map[GPUType]int
	minDurationPassInterval Duration
	dataSourceCSVPath       string
	formatPrintLevel        FormatPrintLevel
}

var defaultOptions = &Options{
	logEnabled: true,
	logDirPath: os.TempDir(),
	gpuType2Count: map[GPUType]int{
		"V100": 1,
		"T4":   1,
		"P100": 1,
	},
	minDurationPassInterval: 1.,
	dataSourceCSVPath:       "",
	formatPrintLevel:        ShortMsgPrint,
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

func WithOptionDataSourceCSVPath(csvPath string) SetOption {
	return func(options *Options) {
		options.dataSourceCSVPath = csvPath
	}
}

func WithOptionFmtPrintLevel(formatPrintLevel FormatPrintLevel) SetOption {
	return func(options *Options) {
		options.formatPrintLevel = formatPrintLevel
	}
}

func WithOptionMinDurationPassInterval(minDurationPassInterval Duration) SetOption {
	return func(options *Options) {
		options.minDurationPassInterval = minDurationPassInterval
	}
}
