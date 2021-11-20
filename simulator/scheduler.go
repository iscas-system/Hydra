package simulator

type Scheduler interface {
	// DoSchedule 每个调度器使用该方法进行调度。
	// 虽然是公开的方法，但是调度的执行时刻实际上是由调度器自己决定的。
	DoSchedule()

	// SetCluster
	// Simulator 会在开始模拟时调用 SetCluster 方法，将集群注入到 Scheduler 当中。
	SetCluster(cluster *Cluster)
	// OnScheduleEvent
	// 由 Simulator 调用的方法，将任何一个可能产生调度的时间传输给 Scheduler ，让它通过事件决定是否进行一次调度。
	OnScheduleEvent(event ScheduleEvent)
	// NextActiveScheduleTime
	// Simulator 会在每次决定需要经过多久时间时，询问 Scheduler ，下一次主动开始调度的时间是何时。
	// 通过这个方法能够解决存在有缓冲区的调度器的调度。如一个调度器可能在内部缓存了一些新来的任务暂时没有把它们调度到集群上，
	// 而这个调度器规定每隔五秒，将缓冲区的任务调度到集群上。这时 Simulator 需要知道下一次 Scheduler 想要主动发起调度的时间，
	// Simulator 知道后，将时间流逝到恰好调度器想要主动发起调度，即可保证 Simulator 没有越过这个时间。
	// 这样才能恰好让调度器在想要主动进行调度时立即调度。
	// 如果它返回一个inf值，则表示这个调度器从不主动进行调度，只有在被动接受 Simulator 传来的事件时才进行调度。
	NextActiveScheduleTime() Time
}
