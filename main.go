package main

import (
	"DES-go/schedulers"
	"DES-go/simulator"
)

func main() {
	//scheduler := schedulers.NewDummyScheduler()
	scheduler := schedulers.NewSJFScheduler(false)
	simu := simulator.NewSimulator(scheduler,
		simulator.WithOptionFmtPrintLevel(simulator.ShortMsgPrint),
		simulator.WithOptionDataSourceCSVPath("/Users/purchaser/go/src/DES-go/cases/case_200.csv"),
		simulator.WithOptionLogEnabled(true),
		simulator.WithOptionLogPath("/Users/purchaser/go/src/DES-go/logs"),
		simulator.WithOptionGPUType2Count(map[simulator.GPUType]int{
			"V100": 10,
			"P100": 10,
			"T4":   10,
		}))
	simu.Start()
}
