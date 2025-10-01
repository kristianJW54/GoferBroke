package cluster

import (
	"testing"
	"time"
)

func TestBucket_RateLimit(t *testing.T) {

	b := &Bucket{
		tokens:     2,
		maxTokens:  2,
		refillRate: 2,
		lastRefill: time.Now(),
	}

	// First 2 calls should succeed
	if !b.Allow() {
		t.Error("Bucket allow fail")
	}
	if !b.Allow() {
		t.Error("Bucket allow fail")
	}

	// Third call immediately should fail
	if b.Allow() {
		t.Fatal("expected third call to be blocked")
	}

	// Wait ~0.5s -> should refill ~1 token (rate=2/sec)
	time.Sleep(500 * time.Millisecond)

	if !b.Allow() {
		t.Fatal("expected call after half a second to pass (1 token refilled)")
	}

	// Next call right away should fail again (token just consumed)
	if b.Allow() {
		t.Fatal("expected immediate follow-up call to be blocked")
	}

}
