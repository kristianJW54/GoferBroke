package cluster

import (
	"context"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log/slog"
	"sync"
	"time"
)

const (
	ALIVE = uint8(iota) + 1
	SUSPECTED
	SUSPECTED_STORED
	FAULTY
)

type failureControl struct {
	mu                    sync.RWMutex
	k                     uint8
	failureTimeout        time.Duration
	gossipTimeout         time.Duration
	maxGossipRoundTimeout time.Duration
	suspectedNodes        map[string]struct{}
	failedNodes           map[string]struct{}
}

type failure struct {
	mu                 sync.RWMutex
	state              uint8
	incarnationVersion int64
	suspectSince       time.Time
}

func newFailure() *failure {
	return &failure{
		state:              ALIVE,
		incarnationVersion: 0,
	}
}

func newFailureControl(conf *GbClusterConfig) *failureControl {

	maxGossipTime := conf.Cluster.GossipRoundTimeout + conf.Cluster.FailureProbeTimeout

	return &failureControl{
		k:                     conf.Cluster.FailureKNodesToProbe,
		failureTimeout:        time.Duration(conf.Cluster.FailureProbeTimeout),
		gossipTimeout:         time.Duration(conf.Cluster.GossipRoundTimeout),
		maxGossipRoundTimeout: time.Duration(maxGossipTime),
		suspectedNodes:        make(map[string]struct{}),
		failedNodes:           make(map[string]struct{}),
	}

}

func (s *GBServer) handleIndirectProbe(ctx context.Context, target string) error {

	// first we need to check if we can indirect probe

	s.configLock.RLock()
	k := s.gbClusterConfig.Cluster.FailureKNodesToProbe
	s.configLock.RUnlock()

	active := s.activeNodeIDs()

	if len(active) == 1 {
		// If here we need to just add the suspect delta to our map and let the background process begin it's cleanup
		// Unless we receive an Alive delta from the suspect
		// Mark suspect here - if premature, suspect node should refute pretty quick as we are a two node cluster

		s.clusterMapLock.RLock()
		part, exists := s.clusterMap.participants[target]
		s.clusterMapLock.RUnlock()
		if !exists {
			return Errors.ChainGBErrorf(Errors.IndirectProbeErr, nil, "target %s doesn't exist in cluster map", target)
		}

		if part.f.state == SUSPECTED || part.f.state == SUSPECTED_STORED {
			return nil
		}

		err := s.markSuspect(target)
		if err != nil {
			return Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "")
		}

		return Errors.ChainGBErrorf(Errors.IndirectProbeErr, nil, "not enough participants to indirect probe -- marking node as suspect")
	}

	// we need to select k helpers to indirectly ping target
	if int(k) > (len(active) - 1) {
		k = uint8(len(active) - 1)
	}

	// now we choose a requester
	r, err := generateRandomParticipantIndexesForGossip(active, int(k), &s.notToGossipNodeStore, target)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "")
	}

	err = s.sendProbes(ctx, r, active, target)
	if err != nil {
		handledErr := Errors.HandleError(err, func(gbErrors []*Errors.GBError) error {

			for _, gbError := range gbErrors {
				switch gbError.Code {
				case Errors.PROBE_FAILED_CODE:
					err = s.markSuspect(target)
					if err != nil {
						return Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "")
					}
					return err
				}
			}

			// Need to handle other potential errors
			return nil

		})
		return handledErr
	}

	return nil

}

// TODO May want to use this for simple probe rather than iterating over k nodes for parallel probing..?
func (s *GBServer) sendSingleProbe(ctx context.Context, requester *gbClient, target string) error {

	probeReqID, _ := requester.srv.acquireReqID()

	name := append([]byte(target), '\r', '\n')

	pay, err := prepareRequest(name, 1, PROBE, probeReqID, uint16(0))
	if err != nil {
		return err
	}

	resp := requester.qProtoWithResponse(ctx, probeReqID, pay, true)

	rsp, err := requester.waitForResponseAndBlock(resp)
	if err != nil {
		// Need to handle the error
		s.logger.Info("just printing the error for visibility", "error", err)
		return err
	}

	if rsp.msg == nil {
		return err
	}

	s.logger.Info("return message", "msg", string(rsp.msg))

	return nil

}

func (s *GBServer) sendProbes(parentCtx context.Context, helpers []int, parts []string, target string) error {

	// build “target\r\n” once
	name := append([]byte(target), '\r', '\n')

	// build done channel and error channel
	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, len(helpers))

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel() // safety: ensures goroutine cleanup on early return

	var wg sync.WaitGroup
	wg.Add(len(helpers))

	for _, h := range helpers {

		helperID := parts[h]

		go func() {
			defer wg.Done()

			// now need to see if we have a client connection of the requester we wish to contact
			client, exists, err := s.getNodeConnFromStore(helperID)
			if err != nil {
				errCh <- Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "for requester %s", helperID)
				return
			}
			if !exists {
				errCh <- Errors.ChainGBErrorf(Errors.IndirectProbeErr, nil, "requester %s does not exist", helperID)
				return
			}

			reqID, err := client.srv.acquireReqID()
			if err != nil {
				errCh <- Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "")
				return
			}

			pay, err := prepareRequest(name, 1, PROBE, reqID, uint16(0))
			if err != nil {
				errCh <- Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "")
				return
			}

			resp := client.qProtoWithResponse(ctx, reqID, pay, true)

			rsp, err := client.waitForResponseAndBlock(resp)
			if err != nil {
				// Need to handle the error
				errCh <- err
				return
			}

			if rsp.msg == nil {
				errCh <- Errors.ChainGBErrorf(Errors.IndirectProbeErr, nil, "expecting response to not be nil")
				return
			}

			select {
			case doneCh <- struct{}{}:
				cancel()
			default:
			}

		}()

	}

	//  - a doneCh signal (one helper succeeded),
	//  - ctx.Done() (timeout or overall cancel),
	//  - or all helpers to report errors
	go func() { wg.Wait(); close(errCh) }()

	failCount := 0

	for {
		select {
		case <-doneCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			if err != nil {
				failCount++
				if failCount == len(helpers) {
					return Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "")
				}
			}
		}
	}

}

// If we mark a node as suspect - we must log it's time since suspected in order to measure if enough time has passed
// between since and now to be more than convergence estimation

func (s *GBServer) markSuspect(node string) error {

	s.clusterMapLock.RLock()
	part, exists := s.clusterMap.participants[node]
	s.clusterMapLock.RUnlock()
	if !exists {
		return Errors.ChainGBErrorf(Errors.MarkSuspectErr, nil,
			"node %s does not exist in cluster map", node)
	}

	part.f.mu.Lock()
	if part.f.state == SUSPECTED || part.f.state == SUSPECTED_STORED {
		part.f.mu.Unlock()
		return nil
	}
	part.f.state = SUSPECTED
	part.f.suspectSince = time.Now()
	part.f.mu.Unlock()

	s.fail.mu.Lock()
	s.fail.suspectedNodes[node] = struct{}{}
	s.fail.mu.Unlock()

	// TODO Do defensive checks here in case it's already suspected or faulty?
	err := s.updateSelfInfo(&Delta{KeyGroup: FAILURE_DKG, Key: node, Version: time.Now().Unix(), ValueType: D_UINT8_TYPE, Value: []byte{SUSPECTED}})
	if err != nil {
		return Errors.ChainGBErrorf(Errors.MarkSuspectErr, err, "for node: %s", node)
	}

	// TODO Finish --
	// We don't immediately stop gossiping with the suspected node - we wait for convergence in the background process
	// And then let that stop the gossip with the suspected - this gives the suspected node time to refute the suspicion

	s.logger.Info("marking node suspected", "node", node)

	return nil

}

//---------------------------------------------------
// Failure checks and refutes for on wire gossip

func (s *GBServer) checkFailureGSA(uuid string, v *Delta) error {

	val := uint8(v.Value[0])

	// First check if we are the ones being reported on for a failure level
	// If we are we need to increment our ALIVE version - we gossip back and refute the claim
	// We return early as we don't need to handle anything more
	if v.Key == s.ServerName {

		err := s.updateSelfInfo(&Delta{KeyGroup: FAILURE_DKG, Key: s.ServerName, ValueType: D_UINT8_TYPE, Version: time.Now().Unix(), Value: []byte{ALIVE}})
		if err != nil {
			return Errors.ChainGBErrorf(Errors.CheckFailureGSAErr, err, "")
		}

		return nil

	}

	s.clusterMapLock.RLock()
	part, exists := s.clusterMap.participants[v.Key]
	s.clusterMapLock.RUnlock()
	if !exists {
		return Errors.ChainGBErrorf(Errors.CheckFailureGSAErr, nil, "%s - does not exist in our cluster map", v.Key)
	}

	switch val {
	case ALIVE:
		// If we have received an alive failure type this will be either from a node we have suspected or an indirect gossip
		// from a node that has been suspected - we are receiving from the initiating node during this round
		// We need to check if we have something different in our view, if we do, then we apply the newer version and unmark as suspected

		// Must check our view of the node
		self := s.GetSelfInfo()
		ourView, exists := self.keyValues[MakeDeltaKey(FAILURE_DKG, v.Key)]
		if !exists {
			return Errors.ChainGBErrorf(Errors.CheckFailureGSAErr, nil, "%s - does not exist in our view", v.Key)
		}

		if ourView.Value[0] == SUSPECTED {
			// TODO Abstract to function so we can also clean up not to gossip store and any background tasks we need to check
			s.logger.Warn("we have been refuted - was SUSPECTED - now ALIVE",
				slog.String("refuted-node", uuid))

			self.pm.Lock()
			*ourView = *v
			if v.Version > self.maxVersion {
				self.maxVersion = v.Version
			}
			self.pm.Unlock()

			// Now we get the suspected *Participant and unmark as suspected

			part.f.mu.Lock()
			part.f.state = ALIVE
			part.f.incarnationVersion = v.Version
			part.f.mu.Unlock()

			return nil

		}

		return nil
	case SUSPECTED:

		s.logger.Info("state of part", "failure", part.f.state)

		if part.f.state == SUSPECTED {
			return nil
		}

		err := s.markSuspect(v.Key)
		if err != nil {
			return Errors.ChainGBErrorf(Errors.CheckFailureGSAErr, err, "")
		}

		return nil

	case FAULTY:
		// Add dead node kv here if we encounter one - tombstone basically and let the background tasks gc
		// Need to make sure we are accessing our view of the dead node

		s.clusterMapLock.RLock()
		cm := s.clusterMap
		s.clusterMapLock.RUnlock()

		dn, exists := cm.participants[v.Key]
		if !exists {
			return Errors.ChainGBErrorf(Errors.CheckFailureGSAErr, nil, "%s - does not exist in our clustermap", v.Key)
		}

		dn.pm.Lock()
		clear(dn.keyValues)
		dn.keyValues = map[string]*Delta{
			MakeDeltaKey(FAILURE_DKG, uuid): {
				KeyGroup:  FAILURE_DKG,
				Key:       v.Key,
				Version:   time.Now().Unix(),
				ValueType: D_BYTE_TYPE,
				Value:     []byte{FAULTY},
			},
		}
		dn.maxVersion = time.Now().Unix()
		dn.pm.Unlock()

		// Update our view of this
		err := s.updateSelfInfo(v)
		if err != nil {
			return Errors.ChainGBErrorf(Errors.CheckFailureGSAErr, err, "")
		}

		// We then need to close and remove the connection to ensure that it no longer appears as active node
		if conn, exists := s.nodeConnStore.LoadAndDelete(v.Key); exists {
			cli, ok := conn.(*gbClient)
			if !ok {
				return Errors.ChainGBErrorf(Errors.CheckFailureGSAErr, nil, "%s - is not a client", v.Key)
			}
			_ = cli.gbc.Close()
		}
		return nil
	}
	return nil
}

//---------------------------------------------------
// Suspect checker - to be used as background job

func (s *GBServer) checkSuspectedNode() {

	// We get the standard convergence estimate here to make sure the SUSPECTED is fully gossiped
	ct := s.getConvergenceEst()

	s.configLock.RLock()
	ft := time.Duration(uint64(s.gbClusterConfig.Cluster.NodeFaultyAfter)) * time.Millisecond
	s.configLock.RUnlock()

	s.fail.mu.RLock()
	suspected := s.fail.suspectedNodes
	s.fail.mu.RUnlock()

	for node := range suspected {

		s.clusterMapLock.RLock()
		part := s.clusterMap.participants[node]
		s.clusterMapLock.RUnlock()

		elapsed := time.Since(part.f.suspectSince)

		// To calculate a fault duration we need to make a refute window which is double the convergence estimate to
		// account for full SUSPECTED gossiped and then full ALIVE refute gossiped if refuted
		fault := elapsed - (ct * 2)

		switch part.f.state {
		case SUSPECTED:
			if elapsed > ct {
				if _, exists := s.notToGossipNodeStore.Load(node); exists {
					return
				}
				s.logger.Warn("moving node to not-to-gossip store...", "node", node, "elapsed", elapsed)
				s.notToGossipNodeStore.Store(node, struct{}{})
				part.f.mu.Lock()
				part.f.state = SUSPECTED_STORED
				part.f.mu.Unlock()

				return
			}
		case SUSPECTED_STORED:
			if fault > ft {
				s.logger.Warn("Node now faulty - tombstoning")
				part.f.mu.Lock()
				part.f.state = FAULTY
				part.f.mu.Unlock()

				s.fail.mu.Lock()
				delete(s.fail.suspectedNodes, node)
				s.fail.failedNodes[node] = struct{}{}
				s.fail.mu.Unlock()

				err := s.updateSelfInfo(&Delta{KeyGroup: FAILURE_DKG, Key: node, Version: time.Now().Unix(), ValueType: D_UINT8_TYPE, Value: []byte{FAULTY}})
				if err != nil {
					return
				}

				part.pm.Lock()
				clear(part.keyValues)
				part.keyValues = map[string]*Delta{
					MakeDeltaKey(FAILURE_DKG, node): {
						KeyGroup:  FAILURE_DKG,
						Key:       node,
						Version:   time.Now().Unix(),
						ValueType: D_BYTE_TYPE,
						Value:     []byte{FAULTY},
					},
				}
				part.maxVersion = time.Now().Unix()
				part.pm.Unlock()

				return

			}
		case ALIVE:
			s.logger.Info("removing node from suspected", "node", node)
			s.notToGossipNodeStore.Delete(node)
			delete(suspected, node)

			return
			// We have already changed the participant in our addGSADeltaToMap method - we don't need to alter participant state here
		default:

		}
	}
}
