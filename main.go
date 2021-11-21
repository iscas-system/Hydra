package main

import (
	"DES-go/schedulers"
	"DES-go/simulator"
)

func main() {
	//scheduler := schedulers.NewDummyScheduler()
	scheduler := schedulers.NewSJFScheduler(true)
	simu := simulator.NewSimulator(scheduler,
		simulator.WithOptionFmtPrintLevel(simulator.ShortMsgPrint),
		simulator.WithOptionDataSourceCSVPath("/Users/purchaser/go/src/DES-go/cases/case_2000.csv"),
		simulator.WithOptionLogEnabled(true),
		simulator.WithOptionLogPath("/Users/purchaser/go/src/DES-go/logs"),
		simulator.WithOptionGPUType2Count(map[simulator.GPUType]int{
			"V100": 1,
			"P100": 1,
			"T4":   1,
		}))
	simu.Start()
}
