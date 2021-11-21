package main

import (
	"DES-go/schedulers"
	"DES-go/simulator"
)

func main() {
	scheduler := schedulers.NewDummyScheduler()
	simu := simulator.NewSimulator(scheduler,
		simulator.WithOptionFmtPrintLevel(simulator.ShortMsgPrint),
		simulator.WithOptionDataSourceCSVPath("/Users/purchaser/go/src/DES-go/cases/case_2000.csv"),
		simulator.WithOptionLogEnabled(true),
		simulator.WithOptionLogPath("/Users/purchaser/go/src/DES-go/logs"),
		simulator.WithOptionGPUType2Count(map[simulator.GPUType]int{
			"V100": 100,
			"P100": 100,
			"T4":   100,
		}))
	simu.Start()
}
