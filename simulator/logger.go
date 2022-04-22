package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type logger struct {
	ctx        context.Context
	cancel     context.CancelFunc
	enabled    bool
	logDirPath string

	logMsgChan chan *loggerMsg
	wg         *sync.WaitGroup
}

type loggerMsg struct {
	finishedJobs []*Job
	simpleString string
}

func NewLogger(enabled bool, logPath string) *logger {
	ctx, cancel := context.WithCancel(context.Background())
	logger := &logger{
		ctx:        ctx,
		cancel:     cancel,
		enabled:    enabled,
		logDirPath: logPath,

		logMsgChan: make(chan *loggerMsg, 0),
		wg:         &sync.WaitGroup{},
	}
	logger.startLogRoutine()
	return logger
}

func (l *logger) Exit() {
	l.cancel()
	l.wg.Wait()
}

func (l *logger) ReceiveFinishedJobs(jobs []*Job) {
	l.logMsgChan <- &loggerMsg{
		finishedJobs: jobs,
	}
}

func (l *logger) ReceiveStringLog(log string) {
	l.logMsgChan <- &loggerMsg{
		finishedJobs: nil,
		simpleString: log,
	}
}

func (l *logger) finishedJobsLogger() func(finishedJobs []*Job) {
	loggedFinishedJobsCount := 0
	return func(finishedJobs []*Job) {
		if len(finishedJobs) == 0 {
			return
		}
		b := &strings.Builder{}
		b.WriteByte('\n')
		genFirstLine, genLastLine := func() (func(c int) string, func(firstLine string) string) {
			sp := strings.Repeat("=", 50)
			return func(c int) string {
					return fmt.Sprintf("%s%s:%d%s\n", sp, "Finished", c, sp)
				}, func(firstLine string) string {
					return strings.Repeat("=", len(firstLine)) + "\n"
				}
		}()
		for _, job := range finishedJobs {
			fl := genFirstLine(loggedFinishedJobsCount)
			b.WriteString(fl)
			bytes, err := json.Marshal(job)
			if err != nil {
				panic(err)
			}
			strJob := string(bytes)
			b.WriteString(strJob)
			b.WriteString(genLastLine(fl))
			loggedFinishedJobsCount++
		}
		log.Printf(b.String())
	}
}

func (l *logger) simpleStringLogger() func(simpleString string) {
	return func(simpleString string) {
		log.Printf(simpleString)
	}
}

func (l *logger) startLogRoutine() {
	l.wg.Add(1)
	go func() {
		logFileName := time.Now().Format("2006-01-02_15:04:05.log")
		logPath := filepath.Join(l.logDirPath, logFileName)
		fp, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
		log.SetOutput(fp)

		finishedJobsLogger := l.finishedJobsLogger()
		simpleStringLogger := l.simpleStringLogger()
		for {
			select {
			case msg := <-l.logMsgChan:
				if msg.simpleString != "" {
					simpleStringLogger(msg.simpleString)
				}
				if msg.finishedJobs != nil {
					finishedJobsLogger(msg.finishedJobs)
				}
			case <-l.ctx.Done():
				log.Printf("logger exit.")
				l.wg.Done()
				return
			}
		}
	}()
}
