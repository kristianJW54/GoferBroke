package cluster

import (
	"fmt"
	"sync"
	"time"
)

// Flow control ensures no single node overwhelms gossip with excessive updates.
// Without it, a "noisy" node can generate so many deltas that every gossip
// message is filled only with its state, leaving little room for other nodes'
// updates to spread. This leads to backlog, staleness, and unfairness.
//
// The solution has two parts:
//  1. Local rate limiting (token bucket):
//     Each node caps how many new deltas it can generate per second, so it
//     cannot outpace what gossip messages can actually carry.
//  2. Fairness during gossip:
//     When two nodes exchange, they share their current rates and adapt toward
//     a balanced value. This prevents one node from monopolizing message
//     capacity and ensures updates from quieter nodes also propagate.
//
// Together, this keeps gossip bounded, fair, and efficient across the cluster.

const (
	OVERFLOW_ADAPTIVE_RATE  = 0.75
	UNDERFLOW_ADAPTIVE_RATE = 0.2
)

// Flow control for update rates per node

type Bucket struct {
	tokens     float64
	maxTokens  float64 // burst size (e.g. MTU)
	refillRate float64 // τp (from FlowControl.maxRate)
	lastRefill time.Time
	mu         sync.RWMutex
}

type FlowControl struct {
	desiredRate float64 // what the node wants (ρp)
	maxRate     float64 // what the node can do (τp), adjusted by AIMD
	bucket      *Bucket
	mu          sync.RWMutex
}

func NewFlowControl() *FlowControl {

	return &FlowControl{
		desiredRate: DEFAULT_MAX_FLOW_RATE,
		maxRate:     DEFAULT_MAX_FLOW_RATE,
		bucket: &Bucket{
			tokens:     DEFAULT_MAX_FLOW_RATE,
			maxTokens:  DEFAULT_MAX_FLOW_RATE,
			refillRate: DEFAULT_MAX_FLOW_RATE,
			lastRefill: time.Now(),
		},
	}
}

func (fc *FlowControl) UpdateRate(overflow, underflow bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	switch {
	case overflow:
		fc.maxRate *= OVERFLOW_ADAPTIVE_RATE // 0.75
	case underflow:
		fc.maxRate += UNDERFLOW_ADAPTIVE_RATE // 0.2
	default:
		// steady state, no adjustment
	}

	// clamp
	if fc.maxRate < 1 {
		fc.maxRate = 1
	}
	if fc.maxRate > fc.bucket.maxTokens {
		fc.maxRate = fc.bucket.maxTokens
	}

	// sync bucket
	fc.bucket.mu.Lock()
	fc.bucket.refillRate = fc.maxRate
	fc.bucket.mu.Unlock()
}

func (b *Bucket) Allow() bool {

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.lastRefill = now

	// refill tokens

	fmt.Printf("tokens - %f, elapsed %f, refill rate %v\n", b.tokens, elapsed, b.refillRate)

	b.tokens += elapsed * b.refillRate
	if b.tokens > b.maxTokens {
		b.tokens = b.maxTokens
		fmt.Printf("tokens - %f\n", b.tokens)
	}

	fmt.Printf("tokens - %f\n", b.tokens)

	// spend a token
	if b.tokens >= 1 {
		b.tokens -= 1
		return true
	}

	return false

}
