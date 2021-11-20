package simulator

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Logger struct {
	ctx     context.Context
	enabled bool
	logPath string

	logMsgChan chan *loggerMsg
}

type loggerMsg struct {
	finishedJobs []*Job
	metrics      string
}

func NewLogger(ctx context.Context, enabled bool, logPath string) *Logger {
	logger := &Logger{
		ctx:     ctx,
		enabled: enabled,
		logPath: logPath,

		logMsgChan: make(chan *loggerMsg),
	}
	logger.startLogRoutine()
	return logger
}

func (l *Logger) ReceiveFinishedJobs(jobs []*Job) {
	l.logMsgChan <- &loggerMsg{
		finishedJobs: jobs,
	}
}

func (l *Logger) ReceiveMetrics(metrics string) {
	l.logMsgChan <- &loggerMsg{
		finishedJobs: nil,
		metrics:      metrics,
	}
}

func (l *Logger) finishedJobsLogger() func(fp *os.File, finishedJobs []*Job) {
	loggedFinishedJobsCount := 0
	return func(fp *os.File, finishedJobs []*Job) {
		b := &strings.Builder{}
		genFirstLine, genLastLine := func() (func(c int) string, func(firstLine string) string) {
			sp := strings.Repeat("=", 50)
			return func(c int) string {
					return sp + strconv.Itoa(c) + sp + "\n"
				}, func(firstLine string) string {
					return strings.Repeat("=", len(firstLine)) + "\n"
				}
		}()
		for _, job := range finishedJobs {
			fl := genFirstLine(loggedFinishedJobsCount)
			b.WriteString(fl)
			strJob, _ := json.Marshal(job)
			b.Write(strJob)
			b.WriteString(genLastLine(fl))
			loggedFinishedJobsCount++
		}
		_, err := fp.WriteString(b.String())
		if err != nil {
			log.Printf("Logger routine, write finished jobMetas log failed, err=[%v]", err)
		}
	}
}

func (l *Logger) metricsLogger() func(fp *os.File, metrics string) {
	return func(fp *os.File, metrics string) {
		_, err := fp.WriteString(metrics)
		if err != nil {
			log.Printf("Logger routine, write metrics failed, err=[%v]", err)
		}
	}
}

func (l *Logger) startLogRoutine() {
	go func() {
		fp, err := os.OpenFile(l.logPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}

		finishedJobsLogger := l.finishedJobsLogger()
		metricsLogger := l.metricsLogger()
		for {
			select {
			case msg := <-l.logMsgChan:
				if msg.metrics != "" {
					metricsLogger(fp, msg.metrics)
				}
				if msg.finishedJobs != nil {
					finishedJobsLogger(fp, msg.finishedJobs)
				}
			case <-l.ctx.Done():
				log.Printf("Logger exit.")
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}()
}
