package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

//============================================
// Logging
//============================================

type LogEntry struct {
	Time    time.Time
	Level   slog.Level
	Message string
	Attrs   []slog.Attr
}

type logBuffer struct {
	entries []*LogEntry
	max     int
	idx     int
	full    bool
	mu      sync.Mutex
}

func newLogBuffer(size int) *logBuffer {
	return &logBuffer{
		entries: make([]*LogEntry, size),
		max:     size,
	}
}

func (lb *logBuffer) Add(entry *LogEntry) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.entries[lb.idx] = entry
	lb.idx = (lb.idx + 1) % lb.max
	if lb.idx == 0 {
		lb.full = true
	}
}

func (lb *logBuffer) Snapshot() []*LogEntry {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	var out []*LogEntry
	if lb.full {
		out = append(out, lb.entries[lb.idx:]...)
		out = append(out, lb.entries[:lb.idx]...)
	} else {
		out = append(out, lb.entries[:lb.idx]...)
	}
	return out
}

func (lb *logBuffer) Drain() []*LogEntry {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	var out []*LogEntry
	if lb.full {
		out = append(out, lb.entries[:lb.idx]...)
		out = append(out, lb.entries[lb.idx:]...)
	} else {
		out = append(out, lb.entries[:lb.idx]...)
	}

	lb.entries = make([]*LogEntry, len(lb.entries))
	lb.idx = 0
	lb.full = false

	return out

}

//----------
// Implement the interface

func (lb *logBuffer) Handle(rec slog.Record, attr []slog.Attr) error {
	entry := &LogEntry{
		Time:    rec.Time,
		Level:   rec.Level,
		Message: rec.Message,
		Attrs:   attr,
	}
	lb.Add(entry)

	return nil

}

type CustomSlogHandler interface {
	Handle(rec slog.Record, attrs []slog.Attr) error
}

type slogLogger struct {
	ch          chan slog.Record
	next        slog.Handler
	customLogic CustomSlogHandler
	done        chan struct{}
	ctx         context.Context
	once        sync.Once
}

func newSlogLogger(ctx context.Context, custom CustomSlogHandler, next slog.Handler, chanSize int) *slogLogger {
	h := &slogLogger{
		ch:          make(chan slog.Record, chanSize),
		next:        next,
		customLogic: custom,
		done:        make(chan struct{}),
		ctx:         ctx,
	}
	go h.run()
	return h
}

// Run as go-routine
func (h *slogLogger) run() {
	for rec := range h.ch {
		var attrs []slog.Attr
		rec.Attrs(func(a slog.Attr) bool {
			attrs = append(attrs, a)
			return true
		})
		_ = h.customLogic.Handle(rec, attrs)
		_ = h.next.Handle(context.Background(), rec)
	}
	close(h.done)
}

func (h *slogLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return true // Always return true and decide later what to do
}

func (h *slogLogger) Handle(_ context.Context, rec slog.Record) error {
	select {
	case h.ch <- rec:
		return nil
	case <-h.ctx.Done():
		return h.ctx.Err()
	default:
		if h.next != nil {
			return h.next.Handle(context.Background(), rec)
		}
		return nil // Drop or log manually
	}
}

func (h *slogLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	return newSlogLogger(h.ctx, h.customLogic, h.next.WithAttrs(attrs), cap(h.ch))
}

func (h *slogLogger) WithGroup(name string) slog.Handler {
	return newSlogLogger(h.ctx, h.customLogic, h.next.WithGroup(name), cap(h.ch))
}

func (h *slogLogger) Close() {
	h.once.Do(func() {
		close(h.ch)
		<-h.done
	})
}

//========================================================
// Null Common Slog for when only Custom is specified

type noopHandler struct{}

func (n noopHandler) Enabled(_ context.Context, _ slog.Level) bool  { return false }
func (n noopHandler) Handle(_ context.Context, _ slog.Record) error { return nil }
func (n noopHandler) WithAttrs(_ []slog.Attr) slog.Handler          { return n }
func (n noopHandler) WithGroup(_ string) slog.Handler               { return n }

//=========================
// Logger setup

func (s *GBServer) SetupLogger() error {

	return nil

}

//============================================
// Metrics structs
//============================================

type GossipRoundSample struct {
	TimeStamp time.Time     `json:"timeStamp"`
	PeerID    string        `json:"peerId"`
	Duration  time.Duration `json:"duration"`
	Deferred  bool          `json:"deferred"`
}

//============================================
// Gossip Round Implementation

type gossipRoundBuffer struct {
	mu     sync.Mutex
	buffer []*GossipRoundSample
	size   int
	next   int
	full   bool
}

// TODO Size from cluster config
func newGossipRoundBuffer(size int) *gossipRoundBuffer {
	return &gossipRoundBuffer{
		buffer: make([]*GossipRoundSample, size),
		size:   size,
		next:   0,
		full:   false,
	}
}

func (b *gossipRoundBuffer) Add(s *GossipRoundSample) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buffer[b.next] = s
	b.next = (b.next + 1) % b.size
	if b.next == 0 {
		b.full = true
	}
}

func (b *gossipRoundBuffer) SnapshotGossipRoundBuffer() []*GossipRoundSample {

	b.mu.Lock()
	defer b.mu.Unlock()

	var out []*GossipRoundSample

	if !b.full {
		// Case: buffer not full — just copy from 0 to next-1
		out = make([]*GossipRoundSample, b.next)
		for i := 0; i < b.next; i++ {
			out[i] = b.buffer[i]
		}
	} else {
		// Case: buffer full — must rotate from `next` to `end`, then from 0 to `next-1`
		out = make([]*GossipRoundSample, b.size)
		pos := 0

		// Copy from next → end
		for i := b.next; i < b.size; i++ {
			out[pos] = b.buffer[i]
			pos++
		}

		// Copy from 0 → next-1
		for i := 0; i < b.next; i++ {
			out[pos] = b.buffer[i]
			pos++
		}
	}

	return out

}
