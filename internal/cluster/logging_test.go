package cluster

import (
	"context"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestBasicServerLog(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ring := newLogBuffer(100)
	console := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	async := newAsyncRingHandler(ctx, ring, console, 248)
	logger := slog.New(async)

	slog.SetDefault(logger)

	logger.Info("Test log message", "module", "test", "step", 1)
	logger.Warn("Something happened", "code", 123)

	// Let the async worker process logs
	time.Sleep(100 * time.Millisecond)

	// Snapshot and assert
	snapshot := ring.Snapshot()

	if len(snapshot) < 2 {
		t.Errorf("expected at least 2 log entries, got %d", len(snapshot))
	}

	// Optional: verify contents
	found := false
	for _, entry := range snapshot {
		log.Printf("entry = %+v", entry)
		if entry.Message == "Test log message" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'Test log message' in ring buffer, not found")
	}

	// Clean shutdown
	async.Close()

}
