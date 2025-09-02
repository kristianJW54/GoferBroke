package cluster

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

//============================================
// Logging
//============================================

type jsonRingBuffer struct {
	mu   sync.Mutex
	data [][]byte
	max  int
	idx  int
	full bool

	buf     *bytes.Buffer
	handler slog.Handler
}

func newJSONRingBuffer(max int) *jsonRingBuffer {

	buf := &bytes.Buffer{}
	h := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	return &jsonRingBuffer{
		data:    make([][]byte, max),
		max:     max,
		idx:     0,
		buf:     buf,
		handler: h,
	}

}

func (jlb *jsonRingBuffer) encode(rec slog.Record) []byte {
	jlb.buf.Reset()
	_ = jlb.handler.Handle(context.Background(), rec)

	out := make([]byte, jlb.buf.Len())
	copy(out, jlb.buf.Bytes())
	return out
}

func (jlb *jsonRingBuffer) add(entry []byte) {
	jlb.mu.Lock()
	defer jlb.mu.Unlock()

	jlb.data[jlb.idx] = entry
	jlb.idx = (jlb.idx + 1) % len(jlb.data)
	if jlb.idx == 0 {
		jlb.full = true
	}
}

func (jlb *jsonRingBuffer) Snapshot() [][]byte {
	jlb.mu.Lock()
	defer jlb.mu.Unlock()

	if !jlb.full {
		return append([][]byte(nil), jlb.data[:jlb.idx]...)
	}
	return append(append([][]byte(nil), jlb.data[jlb.idx:]...), jlb.data[:jlb.idx]...)
}

//----------

type fastLogger struct {
	ch   chan slog.Record
	done chan struct{}
	ctx  context.Context
	once sync.Once
	next slog.Handler

	jsonBuf *jsonRingBuffer // Lock free encode + ring

	muCopy sync.RWMutex

	suppress atomic.Bool

	stream []*streamLogger
}

type streamLogger struct {
	id      string
	logCh   chan []byte
	handler func([]byte) error
}

func newFastLogger(ctx context.Context, next slog.Handler, ring, chanSize int) *fastLogger {

	h := &fastLogger{
		ch:      make(chan slog.Record, chanSize),
		done:    make(chan struct{}),
		ctx:     ctx,
		jsonBuf: newJSONRingBuffer(ring),
		stream:  make([]*streamLogger, 0, 4),
		next:    next,
	}
	go h.run()
	return h
}

// Run as go-routine
func (fl *fastLogger) run() {

	defer close(fl.done)

	for rec := range fl.ch {
		if fl.suppress.Load() {
			continue
		}
		b := fl.jsonBuf.encode(rec)
		fl.jsonBuf.add(b)
		// We would call dispatch here - but need to check for clients or handler...?
		if len(fl.stream) >= cap(fl.stream) {
			fl.dispatch(b)
		}
		if fl.next != nil {
			_ = fl.next.Handle(fl.ctx, rec)
		}
	}
}

func (fl *fastLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return true // Always return true and decide later what to do
}

func (fl *fastLogger) Handle(_ context.Context, rec slog.Record) error {
	select {
	case fl.ch <- rec:
		return nil
	default:

	}
	return nil
}

func (fl *fastLogger) WithAttrs(_ []slog.Attr) slog.Handler { return fl }
func (fl *fastLogger) WithGroup(_ string) slog.Handler      { return fl }

func (fl *fastLogger) Close() {
	fl.once.Do(func() {
		close(fl.ch)
		<-fl.done
	})
}

// SuppressLogs toggles fast skip. Intended for gossip rounds.
func (fl *fastLogger) SuppressLogs(on bool) { fl.suppress.Store(on) }

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

func setupLogger(ctx context.Context, n *GbNodeConfig) (*slog.Logger, *fastLogger, *jsonRingBuffer) {
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
		return root, nil, nil
	} else {

		var async *fastLogger
		async = newFastLogger(ctx, baseHandler, n.Internal.LogChannelSize, n.Internal.LogChannelSize)
		root := slog.New(async)
		slog.SetDefault(root)
		return root, async, async.jsonBuf
	}

}

//==================================================
// Client handling
//==================================================

func (fl *fastLogger) AddStreamLoggerHandler(
	ctx context.Context,
	id string,
	handler func([]byte) error,
) {

	ch := make(chan []byte, 128)

	entry := &streamLogger{
		id:      id,
		logCh:   ch,
		handler: handler,
	}

	fl.muCopy.Lock()
	fl.stream = append(append([]*streamLogger(nil), fl.stream...), entry)
	fl.muCopy.Unlock()

	// Fan-out goroutine for this client
	go func() {

		for {
			select {
			case b := <-ch:

				framed := append(b, '\r', '\n')
				if err := entry.handler(framed); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (fl *fastLogger) dispatch(b []byte) {
	fl.muCopy.RLock()
	list := fl.stream
	fl.muCopy.RUnlock()

	for _, s := range list {
		select {
		case s.logCh <- b: // non-blocking send
		default: // drop if client too slow
		}
	}
}
