package Cluster

import (
	"GoferBroke/internal/Errors"
	"context"
	"log"
	"math"
	"sync"
	"time"
)

//=======================================================
// Phi Accrual Failure Detection
//=======================================================

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

func (s *GBServer) generateDefaultThreshold(windowSize int) int {
	if windowSize == 0 {
		return 0
	}

	if windowSize < 10 {
		return 2
	} else if windowSize < 30 {
		return 4
	} else if windowSize < 100 {
		return 6
	}
	return 8 // very stable, high-confidence threshold
}

func (s *GBServer) initPhiControl() *phiControl {

	var paWindowSize int

	if s.gbClusterConfig.Cluster.paWindowSize == 0 {
		paWindowSize = DEFAULT_PA_WINDOW_SIZE
	} else {
		paWindowSize = s.gbClusterConfig.Cluster.paWindowSize
	}

	// Should be taken from config
	return &phiControl{
		paWindowSize, // TODO Make sure to switch back to the variable paWindowSize
		s.generateDefaultThreshold(paWindowSize),
		make(chan struct{}),
		make(chan bool),
		false,
		setWarmupBucket(paWindowSize),
		sync.RWMutex{},
	}

}

func setWarmupBucket(windowSize int) int {

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

	log.Printf("warmupBucket for %s = %d/%d", participant.name, participant.paDetection.warmupBucket, s.phi.warmUpBucket)

}

func (s *GBServer) initPhiAccrual() *phiAccrual {

	return &phiAccrual{
		lastBeat:     0,
		window:       make([]int64, s.phi.windowSize), //Should be from config or default //TODO change back paWindowSize
		windowIndex:  0,
		score:        0.00,
		warmupBucket: 0,
		dead:         false,
	}
}

// TODO Need to use this to ensure we are not stuck or spinning, leaking etc
func (s *GBServer) tryStartPhiProcess() bool {
	if s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {
		log.Printf("%s - Cannot start phi: shutting down or context canceled", s.ServerName)
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

		if s.serverContext.Err() != nil {
			log.Printf("%s - gossip process exiting due to context cancellation", s.ServerName)
			//s.endGossip()
			s.gossip.gossMu.Unlock()
			return
		}

		// Wait for gossipOK to become true, or until serverContext is canceled.
		if !s.phi.phiOK || !s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {

			log.Printf("waiting for gossip signal...")
			s.gossip.gossSignal.Wait() // Wait until gossipOK becomes true

		}

		if s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {
			log.Printf("PHI - SHUTTING DOWN")
			s.gossip.gossMu.Unlock()
			return
		}

		s.gossip.gossMu.Unlock()

		s.phi.phiOK = s.startPhiProcess()
		//log.Printf("running phi check")

	}

}

func (s *GBServer) startPhiProcess() bool {

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.serverContext.Done():
			log.Printf("phi process exiting due to context cancellation")
			return false
		case ctrl := <-s.phi.phiControl:
			if !ctrl {
				log.Printf("phi stopped through control channel ------------")
				return false
			}
		case <-ticker.C:
			log.Printf("running phi check")

			// TODO We are only calculating once so far - need to fix

			ctx, cancel := context.WithTimeout(s.serverContext, 2*time.Second)

			s.startGoRoutine(s.ServerName, "main-phi-process-check", func() {

				defer cancel()

				s.calculatePhi(ctx)

			})

		}
	}

}

//TODO Need a ready check or warmup phase to ensure we have enough samples for the window size
// Or we remove the zeros for a large window size

//----------------------------------
// Calculation of phi

func (s *GBServer) recordPhi(node string) error {

	s.clusterMapLock.RLock()
	cm := s.clusterMap.participants
	s.clusterMapLock.RUnlock()

	if n, exists := cm[node]; exists {

		now := time.Now().UnixMilli()

		n.pm.RLock()
		p := n.paDetection
		n.pm.RUnlock()

		interval := now - p.lastBeat

		p.pa.Lock()

		if p.lastBeat != 0 {

			p.window[n.paDetection.windowIndex] = interval
			p.windowIndex = (p.windowIndex + 1) % len(p.window)

		}

		p.lastBeat = now
		s.increaseWarmupBucket(cm[n.name])
		p.pa.Unlock()

	} else {
		return Errors.NodeNotFoundErr
	}

	return nil

}

func getMean(array []int64) float64 {
	if len(array) == 0 {
		return 0.0
	}
	var sum float64
	for _, v := range array {
		sum += float64(v)
	}
	return sum / float64(len(array))
}

func getVariance(array []int64) float64 {

	if len(array) == 0 {
		return 0.0
	}

	var sumOfSquaredDiffs float64

	mean := getMean(array)

	for _, v := range array {
		sqrDiff := float64(v) - mean
		sumOfSquaredDiffs += sqrDiff * sqrDiff
	}

	return sumOfSquaredDiffs / float64(len(array))

}

func std(array []int64) float64 {
	return math.Sqrt(getVariance(array))
}

func cdf(mean, std, v float64) float64 {
	return (1.0 / 2.0) * (1 + math.Erf((v-mean)/(std*math.Sqrt2)))
}

func (s *GBServer) warmUpCheck(participant *Participant, array []int64) []int64 {
	switch {
	case participant.paDetection.warmupBucket == 0:
		// Very early: just use 1 sample
		return array[:1]

	case participant.paDetection.warmupBucket == s.phi.windowSize:
		// Fully warmed up
		return array

	default:
		// In warm-up phase: return a small padded window
		warmupBucket := participant.paDetection.warmupBucket
		sliceSize := warmupBucket + 10
		log.Printf("slice size = %v", sliceSize)
		if sliceSize > len(array) {
			sliceSize = len(array)
		}
		return array[:sliceSize]
	}
}

// Phi returns the φ-failure for the given value and distribution.
func phi(v float64, d []int64) float64 {
	if len(d) == 0 {
		return 0.0 // no data
	}

	mean := getMean(d)
	stdDev := std(d)

	if v <= mean || d[0] == 0 {
		return 0.0 // received on time or early
	}

	cdfValue := cdf(mean, stdDev, v)
	p := 1 - cdfValue

	if p < 1e-10 {
		p = 1e-10
	}

	return -math.Log10(p)
}

func (s *GBServer) calculatePhi(ctx context.Context) {

	// Periodically run a phi check on all participant in the cluster
	select {
	case <-ctx.Done():
		log.Printf("%s - phi process exiting due to context cancellation", s.ServerName)
		return
	default:
		// First calculate mean
		s.clusterMapLock.RLock()
		cm := s.clusterMap.participants
		s.clusterMapLock.RUnlock()

		now := time.Now().UnixMilli()

		for _, participant := range cm {

			participant.pm.RLock()
			if participant.name == s.ServerName || participant.paDetection.dead || s.discoveryPhase {
				participant.pm.RUnlock()
				continue
			}
			participant.pm.RUnlock()

			// If we expect other processes to access window then we may need to lock and also copy...
			lastBeat := participant.paDetection.lastBeat
			window := participant.paDetection.window

			phiWindow := s.warmUpCheck(participant, window)

			delta := now - lastBeat

			phi := phi(float64(delta), phiWindow)

			log.Printf("%s - window = %v", s.ServerName, phiWindow)

			log.Printf("%s - phi = %.2f | Δt = %d ms | %s -> %s",
				s.ServerName, phi, delta, s.ServerName, participant.name)
			participant.pm.Lock()
			participant.paDetection.score = phi
			threshold := s.phi.threshold
			if participant.paDetection.warmupBucket <= 10 {
				threshold = 20 // TODO make config - warmUp Threshold -- Maybe want this high because this is warming up and may have skewed numbers
			}
			if phi > float64(threshold) {
				participant.paDetection.dead = true
			}
			participant.pm.Unlock()

		}

		return

	}

}
