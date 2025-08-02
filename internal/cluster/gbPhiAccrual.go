package cluster

import (
	"context"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

//=======================================================
// Phi Accrual Failure Detection
//=======================================================

const (
	DEFAULT_REACH_WAIT = 3 * time.Second // Seconds
	DEFAULT_DEAD_GC    = 10 * time.Second
)

const (
	PHI_SAMPLE_JITTER_MS = 5 // tiny random jitter when we seed
	PHI_CONSECUTIVE_FAIL = 3 // k-of-n votes before “dead”
)

//-------------------
//Heartbeat Monitoring + Failure Detection

type phiControl struct {
	threshold    float64
	warmUpBucket uint16
	windowSize   uint16
	phiSemaphore chan struct{}
	phiControl   chan bool
	phiOK        bool
	mu           sync.RWMutex
}

type phiAccrual struct {
	reachAttempts  uint8
	warmupBucket   uint16 // keep if you still increment it
	lastBeat       int64  // Unix-ms
	mean           float64
	variance       float64
	dead           bool
	suspectedCount uint8
	pa             sync.Mutex
}

// Phi calculates the suspicion level of a participant using the ϕ accrual failure detection model.
//
// From the paper "The ϕ Accrual Failure Detector" (Hayashibara et al., 2004):
//
//     ϕ(t_now) = -log₁₀(P_later(t_now - T_last))
//
// Where:
//   - t_now:      the current timestamp in milliseconds
//   - T_last:     the timestamp when the last heartbeat was received from this participant
//   - Δt = t_now - T_last: time since the last heartbeat
//   - P_later(Δt): the probability that the next heartbeat would arrive *after* Δt,
//                  based on the historical distribution of heartbeat inter-arrival times
//
// A higher ϕ value indicates higher suspicion of failure. Typically, ϕ ≥ 8 implies strong suspicion.
//
// This implementation assumes a normal distribution of inter-arrival times and uses a sliding
// window to estimate the mean and standard deviation for the participant's heartbeat intervals.

// We don't gossip phi scores or node failures to others in the cluster as the detection is based on local time
// It is for other nodes to detect failed nodes themselves

func expectedGapMs(gossipPeriod time.Duration, fanOut, clusterSz int) int64 {
	if fanOut == 0 {
		fanOut = 1
	}
	return int64(gossipPeriod.Milliseconds()) * int64(clusterSz) / int64(fanOut)
}

func (s *GBServer) adjustedThreshold(participant *Participant) float64 {
	warmup := participant.paDetection.warmupBucket

	switch {
	case warmup == 0:
		return 16.0 // disable detection at first
	case warmup < 10:
		return 12.0
	case warmup < 60:
		return 9.9
	case warmup < 80:
		return 9.8
	case warmup >= s.phi.windowSize:
		return 9.7
	default:
		// Between 20 and windowSize
		return 9.7
	}
}

func (s *GBServer) initPhiControl() *phiControl {

	var paWindowSize uint16

	if s.gbClusterConfig.Cluster.PaWindowSize == 0 {
		paWindowSize = DEFAULT_PA_WINDOW_SIZE
	} else {
		paWindowSize = s.gbClusterConfig.Cluster.PaWindowSize
	}

	// Should be taken from config
	return &phiControl{
		9.7,
		setWarmupBucket(paWindowSize),
		paWindowSize,
		make(chan struct{}),
		make(chan bool),
		false,

		sync.RWMutex{},
	}

}

func setWarmupBucket(windowSize uint16) uint16 {

	if windowSize >= DEFAULT_PA_WINDOW_SIZE {
		return 10
	} else if windowSize > 5 {
		return 3
	} else {
		return 1
	}
}

// Lock must be held coming in
func (s *GBServer) increaseWarmupBucket(participant *Participant) {

	if participant.paDetection.warmupBucket < s.phi.windowSize {
		participant.paDetection.warmupBucket++
	}

}

func (s *GBServer) initPhiAccrual() *phiAccrual {
	// expected steady-state gap for ONE peer (will self-adjust later)
	gap := expectedGapMs(s.gossip.gossInterval,
		int(s.gossip.nodeSelection),
		1)

	// add tiny noise so σ ≠ 0
	gap += int64(rand.Intn(PHI_SAMPLE_JITTER_MS))

	return &phiAccrual{
		lastBeat: time.Now().UnixMilli(),
		mean:     float64(gap),
		variance: float64(gap*gap) / 4, // rough initial σ²
	}
}

func (s *GBServer) tryStartPhiProcess() bool {
	if s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {
		fmt.Printf("%s - Cannot start phi: shutting down or context canceled\n", s.PrettyName())
		return false
	}

	select {
	case s.phi.phiSemaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *GBServer) endPhiProcess() {
	select {
	case <-s.phi.phiSemaphore:
	default:
	}
}

func (s *GBServer) phiProcess(ctx context.Context) {
	// Need to wait for the gossip signal to be signalled - here we use the gossipMutex which is the mutex passed to the sync.Cond
	// Also use a defer condition for contextFunc()

	stopCondition := context.AfterFunc(ctx, func() {
		// Notify all waiting goroutines to proceed if needed.
		s.gossip.gossSignal.L.Lock()
		defer s.gossip.gossSignal.L.Unlock()
		s.flags.clear(PHI_STARTED)
		s.flags.set(PHI_EXITED)
	})
	defer stopCondition()

	for {

		s.gossip.gossMu.Lock()

		if s.ServerContext.Err() != nil {
			//s.endGossip()
			s.gossip.gossMu.Unlock()
			return
		}

		// Wait for gossipOK to become true, or until serverContext is canceled.
		if !s.phi.phiOK || !s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {

			fmt.Printf("waiting for gossip signal...\n")
			s.gossip.gossSignal.Wait() // Wait until gossipOK becomes true

		}

		if s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {
			s.gossip.gossMu.Unlock()
			return
		}

		s.gossip.gossMu.Unlock()

		s.phi.phiOK = s.startPhiProcess()

	}

}

func (s *GBServer) startPhiProcess() bool {

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ServerContext.Done():
			return false
		case ctrl := <-s.phi.phiControl:
			if !ctrl {
				return false
			}
		case <-ticker.C:

			ctx, cancel := context.WithTimeout(s.ServerContext, 2*time.Second)

			s.startGoRoutine(s.ServerName, "main-phi-process-check", func() {

				defer cancel()

				s.calculatePhi(ctx)

			})

		}
	}

}

//----------------------------------
// Calculation of phi

// γ controls responsiveness: 0.1 ⇒ about 10 recent samples dominate.
const emaGamma = 0.10

func (p *phiAccrual) updateEMA(sample int64) {
	if p.mean == 0 {
		p.mean = float64(sample)
		p.variance = float64(sample*sample) / 4
		return
	}
	delta := float64(sample) - p.mean
	p.mean += emaGamma * delta
	// Welford-style variance update for EMA
	p.variance = (1 - emaGamma) * (p.variance + emaGamma*delta*delta)
}

func (s *GBServer) seedEMAForAll() {
	gap := expectedGapMs(s.gossip.gossInterval,
		int(s.gossip.nodeSelection),
		len(s.clusterMap.participants))
	for _, n := range s.clusterMap.participants {
		n.paDetection.pa.Lock()
		n.paDetection.mean = float64(gap)
		n.paDetection.variance = float64(gap*gap) / 4
		n.paDetection.pa.Unlock()
	}
}

func (s *GBServer) recordPhi(node string) error {
	s.clusterMapLock.RLock()
	n, ok := s.clusterMap.participants[node]
	s.clusterMapLock.RUnlock()
	if !ok {
		return Errors.NodeNotFoundErr
	}

	now := time.Now().UnixMilli()

	n.pm.RLock()
	p := n.paDetection
	n.pm.RUnlock()

	p.pa.Lock()
	defer p.pa.Unlock()

	if p.lastBeat != 0 {
		interval := now - p.lastBeat
		if interval > 0 {
			p.updateEMA(interval)
		}
	}
	p.lastBeat = now
	s.increaseWarmupBucket(n)
	return nil
}

// Phi returns the φ-failure for the given value and distribution.
func phi(delta, mean, std float64) float64 {
	if std == 0 || delta <= mean {
		return 0
	}
	// one-sided tail probability of a normal distribution
	z := (delta - mean) / std
	p := 0.5 * (1 - math.Erf(z/math.Sqrt2)) // Pr[X > delta]
	if p < 1e-10 {
		p = 1e-10 // avoid –Inf
	}
	return -math.Log10(p)
}

func (s *GBServer) calculatePhi(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	now := time.Now().UnixMilli()

	s.clusterMapLock.RLock()
	participants := s.clusterMap.participants
	s.clusterMapLock.RUnlock()

	for _, participant := range participants {
		if participant.name == s.ServerName {
			continue
		}

		participant.paDetection.pa.Lock()
		mean := participant.paDetection.mean
		variance := participant.paDetection.variance
		lastBeat := participant.paDetection.lastBeat
		participant.paDetection.pa.Unlock()

		std := math.Sqrt(variance)

		delta := now - lastBeat
		phiScore := phi(float64(delta), mean, std)

		threshold := s.phi.threshold

		s.logger.Info("phi", "value", phiScore, "threshold", threshold)

		participant.paDetection.pa.Lock()
		if phiScore > threshold {
			participant.paDetection.suspectedCount++
			if participant.paDetection.suspectedCount >= PHI_CONSECUTIVE_FAIL {
				participant.paDetection.dead = true
				s.logger.Info("node is dead", "participant", participant.name,
					"phi", phiScore, "consecutiveFails", participant.paDetection.suspectedCount)
			}
		} else {
			participant.paDetection.suspectedCount = 0 // reset on any healthy HB
		}
		participant.paDetection.pa.Unlock()
	}
}

//======================================================
// Handling dead nodes
//======================================================

func (s *GBServer) shouldWeGossip(participant *Participant) bool {

	participant.pm.Lock()
	participant.paDetection.reachAttempts++
	participant.pm.Unlock()

	if participant.paDetection.reachAttempts != 3 {
		return true
	} else {
		go s.handleDeadNode(participant)
		return false
	}
}

func (s *GBServer) handleDeadNode(participant *Participant) {

	select {
	case <-s.ServerContext.Done():
		return
	default:
		s.nodeConnStore.Delete(participant.name)
		s.serverLock.Lock()
		s.notToGossipNodeStore[participant.name] = struct{}{}
		s.serverLock.Unlock()

		participant.pm.Lock()
		if p, ok := s.clusterMap.participants[participant.name]; ok {
			p.keyValues = map[string]*Delta{
				MakeDeltaKey(SYSTEM_DKG, _DEAD_): {
					KeyGroup:  SYSTEM_DKG,
					Key:       _DEAD_,
					Version:   time.Now().Unix(),
					ValueType: D_BYTE_TYPE,
					Value:     []byte{},
				},
			}
		}
		participant.pm.Unlock()

		return

	}

}
