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
	trackingFlag atomic.Value
	grWg         *sync.WaitGroup
	routineMap   sync.Map
}

func (g *grTracking) startGoRoutine(serverName, name string, f func()) {
	// Add to the WaitGroup to track the goroutine
	g.grWg.Add(1)

	// Check if tracking is enabled
	if tracking, ok := g.trackingFlag.Load().(bool); ok && tracking {
		// Generate a new unique ID for the goroutine
		id := atomic.AddInt64(&g.index, 1)
		atomic.AddInt64(&g.numRoutines, 1)

		// Log the start of the goroutine
		//log.Printf("%s Starting go-routine %v - %v", serverName, name, id)

		// Store the routine's information in the map
		g.routineMap.Store(id, name)

		// Launch the goroutine
		go func() {
			defer g.grWg.Done()
			defer func() {
				// Recover from any panic that may occur in the goroutine
				if r := recover(); r != nil {
					fmt.Printf("Recovered panic in goroutine %s: %v\n", name, r)
				}

				// If tracking is enabled, decrement the number of routines and remove from the map
				atomic.AddInt64(&g.numRoutines, -1)
				g.routineMap.Delete(id)
				log.Printf("%s Ending go-routine %v - %v", serverName, name, id)

			}()

			// Run the provided function for the goroutine
			f()
		}()
	} else {
		go func() {
			f()
		}()
	}

}

func (g *grTracking) logActiveGoRoutines() {
	log.Printf("Go routines left in tracker: %v", atomic.LoadInt64(&g.numRoutines))
	log.Println("Go routines in tracker:")

	g.routineMap.Range(func(key, value interface{}) bool {
		log.Printf("Goroutine -- %v %s\n", key, value)
		return true // continue iteration
	})
}
