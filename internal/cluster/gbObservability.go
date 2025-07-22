package cluster

import (
	"bytes"
	"context"
	"log/slog"
	"os"
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
	Data    string
}

type normalLogBuffer struct {
	entries []*LogEntry
	max     int
	idx     int
	full    bool
	mu      sync.Mutex
}

type JsonLogEntry struct {
	Data string
}

type jsonLogBuffer struct {
	entries []*JsonLogEntry
	max     int
	idx     int
	full    bool
	mu      sync.Mutex
}

func newLogBuffer(size int) *normalLogBuffer {
	return &normalLogBuffer{
		entries: make([]*LogEntry, size),
		max:     size,
	}
}

func newJsonLogBuffer(size int) *jsonLogBuffer {
	return &jsonLogBuffer{
		entries: make([]*JsonLogEntry, size),
		max:     size,
	}
}

func (lb *normalLogBuffer) Add(entry *LogEntry) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.entries[lb.idx] = entry
	lb.idx = (lb.idx + 1) % lb.max
	if lb.idx == 0 {
		lb.full = true
	}
}

func (jlb *jsonLogBuffer) Add(entry *JsonLogEntry) {
	jlb.mu.Lock()
	defer jlb.mu.Unlock()

	jlb.entries[jlb.idx] = entry
	jlb.idx = (jlb.idx + 1) % jlb.max
	if jlb.idx == 0 {
		jlb.full = true
	}
}

func snapshotRing[T any](ring []T, idx int, full bool) []T {
	var out []T
	if full {
		out = append(out, ring[idx:]...)
		out = append(out, ring[:idx]...)
	} else {
		out = append(out, ring[:idx]...)
	}
	return out
}

func (lb *normalLogBuffer) Snapshot() []*LogEntry {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return snapshotRing(lb.entries, lb.idx, lb.full)
}

func (jlb *jsonLogBuffer) Snapshot() []string {
	jlb.mu.Lock()
	defer jlb.mu.Unlock()

	entries := snapshotRing(jlb.entries, jlb.idx, jlb.full)

	var out []string
	for _, entry := range entries {
		if entry.Data != "" {
			out = append(out, entry.Data)
		}
	}
	return out
}

func (lb *normalLogBuffer) Drain() []*LogEntry {
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

func (lb *normalLogBuffer) Handle(ctx context.Context, rec slog.Record, attr []slog.Attr) error {
	entry := &LogEntry{
		Time:    rec.Time,
		Level:   rec.Level,
		Message: rec.Message,
		Attrs:   attr,
	}
	lb.Add(entry)

	return nil

}

func (jlb *jsonLogBuffer) Handle(ctx context.Context, rec slog.Record, attr []slog.Attr) error {

	var buf bytes.Buffer
	handle := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	_ = handle.Handle(ctx, rec)

	entry := &JsonLogEntry{Data: buf.String()}

	jlb.Add(entry)

	return nil

}

type CustomSlogHandler interface {
	Handle(ctx context.Context, rec slog.Record, attrs []slog.Attr) error
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

		if err := h.customLogic.Handle(h.ctx, rec, attrs); err != nil {
		}
		if h.next != nil {
			_ = h.next.Handle(h.ctx, rec)
		}
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
	//case <-h.ctx.Done():
	//	return h.ctx.Err()
	default:
		if h.next != nil {
			return h.next.Handle(h.ctx, rec)
		}
		return nil
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

func parseLoggerConfigOutput(output string) string {
	switch output {
	case "stdout":
		return "stdout"
	case "stderr":
		return "stderr"
	case "json":
		return "json"
	default:
		return "json"
	}
}

func getLogBufferOutput(output string) string {
	switch output {
	case "json":
		return "json"
	case "JSON":
		return "json"
	default:
		return "text"
	}
}

func setupLogger(ctx context.Context, n *GbNodeConfig) (*slog.Logger, *slogLogger, *normalLogBuffer, *jsonLogBuffer) {

	var logger slog.Handler
	if n.Internal.DefaultLoggerEnabled {
		output := parseLoggerConfigOutput(n.Internal.Output)
		switch output {
		case "stdout":
			logger = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelInfo,
			})
		case "stderr":
			logger = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level: slog.LevelInfo,
			})
		case "json":
			logger = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
				Level: slog.LevelInfo,
			})
		}
	}
	if !n.Internal.LogToBuffer {
		logger := slog.New(logger)
		slog.SetDefault(logger)
		return logger, nil, nil, nil

	} else {

		var async *slogLogger

		if getLogBufferOutput(n.Internal.LogBufferOutput) == "json" {
			ring := newJsonLogBuffer(int(n.Internal.LogBufferSize))
			async = newSlogLogger(ctx, ring, logger, int(n.Internal.LogBufferSize))
			logger := slog.New(async)
			return logger, async, nil, ring
		} else {
			ring := newLogBuffer(int(n.Internal.LogBufferSize))
			async = newSlogLogger(ctx, ring, logger, int(n.Internal.LogChannelSize))
			logger := slog.New(async)
			return logger, async, ring, nil
		}
	}
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
