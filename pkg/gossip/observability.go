package gossip

import (
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"time"
)

// Collector interface with metrics methods??
// EnableMetrics method??

// Expose http handler for user to use
// Collector interface is called which produces a snapshot that the user can send via pull

// Config options
// - EnableMetrics bool
// - Push/Pull ?
// - Url if push ?
// - Interval for collections

///-- Metrics to track

/*
total exchanges
rate per x
round duration ms


deltas_sent
deltas_applied
deltas_dropped

digest_size_bytes
deltas_size_bytes

mtu_overflow_count
overflow_ratio - overflow_count/delta_sent

flow_control_increase_count
flow_control_decrease_count
rate_limit_t - flow control
rate_limit_p - flow control

how_many_dead_nodes


*/

//============================================
// Metrics structs
//============================================

type GossipRoundSample struct {
	TimeStamp time.Time     `json:"timeStamp"`
	PeerID    string        `json:"peerId"`
	Duration  time.Duration `json:"duration"`
	Deferred  bool          `json:"deferred"`
}

type RoundSnapshot struct {
	Rounds []GossipRoundSample `json:"rounds"`
}

func toExported(gsr *cluster.GossipRoundSample) GossipRoundSample {
	return GossipRoundSample{
		TimeStamp: gsr.TimeStamp,
		PeerID:    gsr.PeerID,
		Duration:  gsr.Duration,
		Deferred:  gsr.Deferred,
	}
}

func RoundDurationSnapshot(in []*cluster.GossipRoundSample) RoundSnapshot {
	out := make([]GossipRoundSample, 0, len(in))
	for _, s := range in {
		out = append(out, toExported(s))
	}
	return RoundSnapshot{Rounds: out}
}
