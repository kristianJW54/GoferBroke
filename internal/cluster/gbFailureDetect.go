package cluster

import (
	"context"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"sync"
	"time"
)

type FailureState uint8

const (
	ALIVE FailureState = iota + 1
	SUSPECTED
	FAULTY
)

func (f FailureState) worseThan(other FailureState) bool { return f > other }

type failureControl struct {
	mu                    sync.RWMutex
	k                     uint8
	failureTimeout        time.Duration
	gossipTimeout         time.Duration
	maxGossipRoundTimeout time.Duration
}

type failure struct {
	mu                 sync.RWMutex
	state              FailureState
	retries            uint8
	incarnationVersion int64
	suspectSince       time.Time
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

func (s *GBServer) handleIndirectProbe(ctx context.Context, target string) error {

	// first we need to check if we can indirect probe
	s.clusterMapLock.RLock()
	ntg := s.notToGossipNodeStore
	s.clusterMapLock.RUnlock()

	s.configLock.RLock()
	k := s.gbClusterConfig.Cluster.FailureKNodesToProbe
	s.configLock.RUnlock()

	active := s.activeNodeIDs()

	if len(active) == 2 {
		// If here we need to just add the suspect delta to our map and let the background process begin it's cleanup
		// Unless we receive an Alive delta from the suspect
		return Errors.ChainGBErrorf(Errors.IndirectProbeErr, nil, "not enough participants to indirect probe")
	}

	// we need to select k helpers to indirectly ping target
	if int(k) > (len(active) - 2) {
		k = uint8(len(active) - 2)
	}

	// now we choose a requester
	r, err := generateRandomParticipantIndexesForGossip(active, int(k), ntg, target)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "")
	}

	for _, t := range r {
		s.logger.Info("requesters", "name", active[t])
	}

	// now need to see if we have a client connection of the requester we wish to contact
	client, exists, err := s.getNodeConnFromStore(active[r[0]])
	if err != nil {
		//errCh <- Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "for requester %s", active[r[0]])
		return err
	}
	if !exists {
		//errCh <- Errors.ChainGBErrorf(Errors.IndirectProbeErr, nil, "requester %s does not exist", helperID)
		return err
	}

	err = s.sendSingleProbe(ctx, client, target)
	if err != nil {
		return err
	}

	//err = s.sendProbes(ctx, r, active, target)
	//if err != nil {
	//	s.logger.Info("✗ sendProbes failed – skipping dump", "err", err)
	//	return err
	//}

	return nil

}

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

			s.logger.Info("using helper", "name", helperID)

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

			s.logger.Info("is client nil?", "client", client.name)

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
				s.logger.Info("just printing the error for visibility", "error", err)
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
					return Errors.ChainGBErrorf(Errors.IndirectProbeErr, err, "all helpers failed")
				}
			}
		}
	}

}

func (s *GBServer) markSuspect() error {

	return nil

}
