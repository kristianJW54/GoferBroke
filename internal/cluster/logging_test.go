package cluster

import (
	"context"
	"fmt"
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
	async := newSlogLogger(ctx, ring, console, 256)

	logger := slog.New(async)

	slog.SetDefault(logger)

	// Write logs
	logger.Info("Test log message", "module", "test", "step", 1)
	logger.Warn("Something happened", "code", 123)

	// Optional: wait briefly or rely on async.Close() drain
	time.Sleep(100 * time.Millisecond)

	// Snapshot and assert
	snapshot := ring.Snapshot()

	if len(snapshot) < 2 {
		t.Errorf("expected at least 2 log entries, got %d", len(snapshot))
	}

	for _, snap := range snapshot {
		fmt.Printf("snapshot -----> %s\n", snap)
	}

}
