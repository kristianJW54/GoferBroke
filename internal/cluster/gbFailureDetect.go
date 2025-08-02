package cluster

import (
	"sync"
	"time"
)

type FailureState uint8

const (
	ALIVE FailureState = iota + 1
	SUSPECTED
	FAULTY
	DEAD
)

type failureControl struct {
	mu                    sync.RWMutex
	k                     uint8
	failureTimeout        time.Duration
	gossipTimeout         time.Duration
	maxGossipRoundTimeout time.Duration
}

type failure struct {
	mu    sync.RWMutex
	state FailureState
}

func newFailureControl(conf *GbClusterConfig) *failureControl {

	maxGossipTime := conf.Cluster.GossipRoundTimeout + conf.Cluster.FailureProbeTimeout

	return &failureControl{
		k:                     conf.Cluster.FailureKNodesToProbe,
		failureTimeout:        time.Duration(conf.Cluster.FailureProbeTimeout),
		gossipTimeout:         time.Duration(conf.Cluster.GossipRoundTimeout),
		maxGossipRoundTimeout: time.Duration(maxGossipTime),
	}

}
