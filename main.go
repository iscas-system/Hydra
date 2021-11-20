package DES_go

import (
	"DES-go/schedulers"
	"DES-go/simulator"
)

func main() {
	scheduler := schedulers.NewDummyScheduler()
	simulator.NewSimulator(scheduler,
		simulator.WithOptionLogEnabled(true),
		simulator.WithOptionLogPath("/Users/purchaser/go/src/DES-go"),
		simulator.WithOptionGPUType2Count(map[simulator.GPUType]int{
			"V100": 1,
			"P100": 1,
			"T4":   1,
		}))
}
