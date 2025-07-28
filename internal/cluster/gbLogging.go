package cluster

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

func (lb *normalLogBuffer) Handle(ctx context.Context, rec slog.Record, attr []slog.Attr) (string, error) {
	entry := &LogEntry{
		Time:    rec.Time,
		Level:   rec.Level,
		Message: rec.Message,
		Attrs:   attr,
	}
	lb.Add(entry)

	return entry.String(), nil

}

func (l LogEntry) String() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("[%s] [%s] %s", l.Time.Format(time.RFC3339), l.Level.String(), l.Message))

	for _, attr := range l.Attrs {
		sb.WriteString(fmt.Sprintf(" %s=%v", attr.Key, attr.Value))
	}

	return sb.String()
}

func (jlb *jsonLogBuffer) Handle(ctx context.Context, rec slog.Record, attr []slog.Attr) (string, error) {

	var buf bytes.Buffer
	handle := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	_ = handle.Handle(ctx, rec)

	entry := &JsonLogEntry{Data: buf.String()}

	jlb.Add(entry)

	return buf.String(), nil

}

type CustomSlogHandler interface {
	Handle(ctx context.Context, rec slog.Record, attrs []slog.Attr) (string, error)
}

type slogLogger struct {
	ch          chan slog.Record
	next        slog.Handler
	customLogic CustomSlogHandler
	done        chan struct{}
	ctx         context.Context
	once        sync.Once
	logDispatch *logDispatcher
}

func newSlogLogger(ctx context.Context, custom CustomSlogHandler, next slog.Handler, chanSize int) *slogLogger {

	h := &slogLogger{
		ch:          make(chan slog.Record, chanSize),
		next:        next,
		customLogic: custom,
		done:        make(chan struct{}),
		ctx:         ctx,
		logDispatch: newLogDispatcher(),
	}
	go h.run()
	return h
}

// Run as go-routine
func (h *slogLogger) run() {

	defer close(h.done)

	for rec := range h.ch {
		var attrs []slog.Attr
		rec.Attrs(func(a slog.Attr) bool {
			attrs = append(attrs, a)
			return true
		})

		if s, err := h.customLogic.Handle(h.ctx, rec, attrs); err == nil {
			h.dispatchLogToStream(s)
		}
		if h.next != nil {
			_ = h.next.Handle(h.ctx, rec)
		}
	}
}

func (h *slogLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return true // Always return true and decide later what to do
}

func (h *slogLogger) Handle(_ context.Context, rec slog.Record) error {
	select {
	case h.ch <- rec:
		return nil
	case <-h.done: // Closed or shutting down
		return nil
	default:
		return nil // Optional: log drop strategy
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
	var baseHandler slog.Handler
	if n.Internal.DefaultLoggerEnabled {
		output := parseLoggerConfigOutput(n.Internal.LogOutput)
		switch output {
		case "stdout":
			baseHandler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
		case "stderr":
			baseHandler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
		case "json":
			baseHandler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
		}
	} else {
		baseHandler = noopHandler{}
	}

	if !n.Internal.LogToBuffer {
		root := slog.New(baseHandler)
		slog.SetDefault(root)
		return root, nil, nil, nil
	} else {

		var async *slogLogger

		if getLogBufferOutput(n.Internal.LogBufferOutput) == "json" {
			ring := newJsonLogBuffer(int(n.Internal.LogBufferSize))
			async = newSlogLogger(ctx, ring, baseHandler, int(n.Internal.LogBufferSize))
			root := slog.New(async)
			return root, async, nil, ring
		} else {
			ring := newLogBuffer(int(n.Internal.LogBufferSize))
			async = newSlogLogger(ctx, ring, baseHandler, int(n.Internal.LogChannelSize))
			root := slog.New(async)
			return root, async, ring, nil
		}
	}
}

//==================================================
// Client handling
//==================================================

// We want a handler to stream logs to clients waiting on that channel

type streamLogger struct {
	id      string
	logCh   chan string
	handler func(string) error
}

type logDispatcher struct {
	mu       *sync.Mutex
	handlers []*streamLogger
}

func newLogDispatcher() *logDispatcher {
	return &logDispatcher{
		handlers: make([]*streamLogger, 0, 2),
		mu:       &sync.Mutex{},
	}
}

// TODO Finish and test

func (h *slogLogger) AddStreamLoggerHandler(ctx context.Context, id string, handler func(string) error) (string, error) {

	ch := make(chan string, 128)

	entry := &streamLogger{
		id:      id,
		logCh:   ch,
		handler: handler,
	}

	h.logDispatch.mu.Lock()
	h.logDispatch.handlers = append(h.logDispatch.handlers, entry)
	h.logDispatch.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {

		defer func() {
			if r := recover(); r != nil {
			}
		}()

		wg.Done()

		for {
			select {
			case l := <-ch:
				if err := handler(l); err != nil {
				}
			case <-ctx.Done():
				return
			}
		}

	}()

	wg.Wait()

	return id, nil
}

func (h *slogLogger) dispatchLogToStream(log string) {

	h.logDispatch.mu.Lock()
	defer h.logDispatch.mu.Unlock()

	if len(h.logDispatch.handlers) != 0 {

		for _, l := range h.logDispatch.handlers {

			select {
			case l.logCh <- log:
				// Delivered
			default:
				// Drop
			}

		}

	}

}
