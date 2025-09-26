package cluster

import (
	"sync"
	"time"
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
	desiredRate float64 // what the node *wants* (ρp)
	maxRate     float64 // what the node *can* do (τp), adjusted by AIMD
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
