package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestEventBasic(t *testing.T) {

	now := time.Now().Unix()

	newer := map[string]*Delta{
		"test:key1": &Delta{KeyGroup: TEST_DKG, Key: "key1", ValueType: STRING_V, Version: now, Value: []byte("I am a delta blissfully unaware as to how annoying I am to code")},
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: STRING_V, Version: now, Value: []byte("Try to gossip about me and see what happens")},
	}

	outdated := map[string]*Delta{
		"test:key1": &Delta{KeyGroup: TEST_DKG, Key: "key1", ValueType: STRING_V, Version: now - 2, Value: []byte("I am a delta blissfully")},
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: STRING_V, Version: now - 1, Value: []byte("I Don't Like Gossipers")},
	}

	gbs := GenerateDefaultTestServer("node-1", newer, 1)

	gbs.clusterMap.participants["node-2"] = &Participant{
		name:       "node-2",
		keyValues:  outdated,
		maxVersion: outdated["test:key2"].Version - 2,
	}

	gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

	gbs2 := GenerateDefaultTestServer("node-2", outdated, 1)

	gbs2.clusterMap.participants["node-1"] = &Participant{
		name:       "node-1",
		keyValues:  cloneKeyValues(outdated),
		maxVersion: outdated["test:key2"].Version - 2,
	}

	gbs2.clusterMap.participantArray = append(gbs2.clusterMap.participantArray, "node-1")

	// Register Event and handler

	gbs.event = NewEventDispatcher()
	gbs2.event = NewEventDispatcher()

	// add handler for gbs2, so we can be sent the updated delta event
	// this should start a go-routine which will listen on an event channel for that handler

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := gbs2.AddHandler(ctx, DeltaUpdated, true, func(event Event) error {
		return gbs2.event.HandleDeltaUpdateEvent(event)
	}); err != nil {
		t.Errorf("error adding event: %v", err)
	}

	d, _, err := gbs2.generateDigest()
	if err != nil {
		t.Fatalf("Error generating digest: %v", err)
	}

	name, fd, err := deSerialiseDigest(d)
	if err != nil {
		t.Fatalf("Error serialising digest: %v", err)
	}

	// Prepare GSA
	gsa, err := gbs.prepareGossSynAck(name, fd)
	if err != nil {
		t.Errorf("prepareGossSynAck failed: %v", err)
	}

	_, _, newCd, err := deserialiseGSA(gsa)
	if err != nil {
		t.Errorf("deserialise GSA failed: %v", err)
	}

	if newCd != nil {

		err = gbs2.addGSADeltaToMap(newCd)
		if err != nil {
			t.Errorf("addGSADeltaToMap failed: %v", err)
		}

	}

	time.Sleep(10 * time.Millisecond)
	ctx.Done()

}

func TestAsyncErrorEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wait := make(chan struct{})
	eventReceived := false
	processStopped := false
	expectedCaller := "TestAsyncErrorEvent"

	gbs := &GBServer{
		event:               NewEventDispatcher(),
		ServerContext:       ctx,
		serverContextCancel: cancel,
	}

	// Register internal handler
	if _, err := gbs.addInternalHandler(gbs.ServerContext, InternalError, func(event Event) error {
		defer close(wait)

		errEvent, ok := event.Payload.(*ErrorEvent)
		if !ok {
			return fmt.Errorf("expected *ErrorEvent, got %T", event.Payload)
		}

		if errEvent.Caller != expectedCaller {
			return fmt.Errorf("expected caller %q, got %q", expectedCaller, errEvent.Caller)
		}

		if errEvent.Error == nil || errEvent.Error.Error() == "" {
			return fmt.Errorf("expected non-nil error in ErrorEvent")
		}

		eventReceived = true
		return nil
	}); err != nil {
		t.Fatalf("failed to register internal handler: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Start process
	go func() {
		err := errors.New("simulated error")
		errorEvent := Event{
			EventType: InternalError,
			Time:      time.Now().Unix(),
			Payload: &ErrorEvent{
				ErrorType: TestError,
				Severity:  CollectAndAct,
				Error:     err,
				Caller:    expectedCaller,
			},
			Message: "simulated error event",
		}
		gbs.DispatchEvent(errorEvent)
	}()

	time.Sleep(1 * time.Second)

	// Monitor stop
	select {
	case <-wait:
		log.Println("error received, stopping process")
		processStopped = true
		return
	case <-ctx.Done():
		t.Log("context canceled before wait signaled")
	}

	// Now assert properly
	if !eventReceived {
		t.Errorf("expected event to be received but wasn't")
	}
	if !processStopped {
		t.Errorf("expected process to stop on error but it didn't")
	}
}
