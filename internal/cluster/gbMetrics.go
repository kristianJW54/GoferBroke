package cluster

import (
	"context"
	"fmt"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"sync"
	"time"
)

type gossipRoundEvent struct {
	timestamp        time.Time
	peer             string
	deltasSent       int
	deltasReceived   int
	duration         time.Duration
	deadlineExceeded bool
}

type gossipRoundEventBuffer struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	events []*gossipRoundEvent
	ch     chan *gossipRoundEvent
	max    int
	idx    int
	full   bool
}

func newGossipRoundEventBuffer(max int) *gossipRoundEventBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	buf := &gossipRoundEventBuffer{
		events: make([]*gossipRoundEvent, max),
		ch:     make(chan *gossipRoundEvent, 1024), // or tunable
		max:    max,
		ctx:    ctx,
		cancel: cancel,
	}
	go buf.run()
	return buf
}

func (b *gossipRoundEventBuffer) addToBuffer(e *gossipRoundEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events[b.idx] = e
	b.idx = (b.idx + 1) % len(b.events)
	if b.idx == 0 {
		b.full = true
	}
}

func (b *gossipRoundEventBuffer) Add(e *gossipRoundEvent) {
	select {
	case b.ch <- e:
	case <-b.ctx.Done():
		// Drop silently
	default:
		// Optionally track overflow
	}
}

func (b *gossipRoundEventBuffer) run() {
	for {
		select {
		case e := <-b.ch:
			b.addToBuffer(e)
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *gossipRoundEventBuffer) Snapshot() []*gossipRoundEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	var out []*gossipRoundEvent
	if b.full {
		out = append(out, b.events[b.idx:]...)
		out = append(out, b.events[:b.idx]...)
	} else {
		out = append(out, b.events[:b.idx]...)
	}
	return out
}

func (b *gossipRoundEventBuffer) Summary() (avg time.Duration, max, min time.Duration, count int) {
	evts := b.Snapshot()
	if len(evts) == 0 {
		return
	}

	var total time.Duration
	max, min = evts[0].duration, evts[0].duration
	for _, e := range evts {
		d := e.duration
		total += d
		if d > max {
			max = d
		}
		if d < min {
			min = d
		}
	}
	avg = total / time.Duration(len(evts))
	count = len(evts)
	return
}

//===========================================================
// gopsutil system metrics

type systemMetrics struct {
	hostName       string
	uptime         uint64
	procs          uint64
	os             string
	platform       string
	platformFamily string
	hostID         string
	totalMemory    uint64
	freeMemory     uint64
	usedMemory     uint64
	usedPercent    float64
	cpuVendorID    string
	cpuFamily      string
	cores          int32
	modelName      string
	mhz            float64
	cacheSize      int32
}

// New metrics will just be function calling the gopsutil functions and assigning
// We'll then need a background process ticker to update these and check if they go beyond a bound to then update
// e.g. if memory usage increases by 10% then we update or if used increased by 1000
// then we'll need to have a listener on that channel to handle the changes and update our own deltas

// While we do this - use the process to also check for other updates to our server like resolving addr and making sure it's up-to-date and reachable

func newSystemMetrics() *systemMetrics {

	h, _ := host.Info()
	c, _ := cpu.Info()
	v, _ := mem.VirtualMemory()

	sm := &systemMetrics{
		hostName:       h.Hostname,
		uptime:         h.Uptime,
		procs:          h.Procs,
		os:             h.OS,
		platform:       h.Platform,
		platformFamily: h.PlatformFamily,
		hostID:         h.HostID,
		totalMemory:    v.Total,
		freeMemory:     v.Free,
		usedMemory:     v.Used,
		usedPercent:    v.UsedPercent,
		cpuVendorID:    c[0].VendorID,
		cpuFamily:      c[0].Family,
		cores:          c[0].Cores,
		modelName:      c[0].ModelName,
		mhz:            c[0].Mhz,
		cacheSize:      c[0].CacheSize,
	}

	return sm

}

func getMemory() {

	v, _ := mem.VirtualMemory()

	fmt.Println(host.Info())

	fmt.Println(cpu.Info())

	// almost every return value is a struct
	fmt.Printf("Total: %v, Free:%v, Used:%v, UsedPercent:%f%%\n", v.Total, v.Free, v.Used, v.UsedPercent)

}
