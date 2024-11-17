package src

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type grTracking struct {
	numRoutines  int64
	index        int64
	routineMap   sync.Map
	trackingFlag atomic.Value
	grLock       sync.RWMutex
}

func (g *grTracking) startGoRoutine(name string, wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	if tracking, ok := g.trackingFlag.Load().(bool); ok && tracking {
		g.grLock.Lock()
		id := atomic.AddInt64(&g.index, +1)
		atomic.AddInt64(&g.numRoutines, +1)
		g.routineMap.Store(id, name)
		log.Printf("starting go-routine %v - %v", name, id)
		g.grLock.Unlock()

		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Recovered panic in goroutine %s: %v\n", name, r)
				}
				g.grLock.Lock()
				atomic.AddInt64(&g.numRoutines, -1)
				g.routineMap.Delete(id)
				log.Printf("ending go-routine %v - %v", name, id)
				g.grLock.Unlock()
			}()
			f()
		}()
	} else {
		f()
		//wg.Done()
	}
}
