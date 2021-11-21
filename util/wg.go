package util

import "sync"

func GoWithWG(wg *sync.WaitGroup, idx int, f func(idx int)) {
	wg.Add(1)
	go func() {
		f(idx)
		wg.Done()
	}()
}
