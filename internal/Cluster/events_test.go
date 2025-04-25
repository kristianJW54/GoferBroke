package Cluster

import (
	"context"
	"errors"
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

	gbs2.AddHandler(ctx, DeltaUpdated, true, func(event Event) {
		gbs2.event.HandleDeltaUpdateEvent(event)
	})

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

	wait := make(chan struct{})
	eventReceived := false
	processStopped := false
	expectedCaller := "TestAsyncErrorEvent"

	gbs := &GBServer{
		event:               NewEventDispatcher(),
		ServerContext:       ctx,
		serverContextCancel: cancel,
	}

	time.Sleep(1 * time.Second)

	gbs.addInternalHandler(gbs.ServerContext, InternalError, func(event Event) {

		defer close(wait)

		errEvent, ok := event.Payload.(*ErrorEvent)
		if !ok {
			t.Errorf("Expected *ErrorEvent, got %T", event.Payload)
			return
		}

		if errEvent.Caller != expectedCaller {
			t.Errorf("Expected caller %q, got %q", expectedCaller, errEvent.Caller)
			return
		}

		if errEvent.Error == nil || errEvent.Error.Error() == "" {
			t.Errorf("Expected non-nil error in ErrorEvent")
			return
		}

		eventReceived = true

	})

	normalProcess := func() {

		time.Sleep(1 * time.Second)

		go func() {

			err := errors.New("I am an error :) handle me carefully please")
			errorEvent := Event{
				InternalError,
				time.Now().Unix(),
				&ErrorEvent{
					TestError,
					CollectAndAct,
					err,
					expectedCaller,
				},
				"This is an error",
			}

			gbs.DispatchEvent(errorEvent)

		}()

		go func() {

			time.Sleep(1 * time.Second)

			for {
				select {
				case <-wait:
					log.Printf("error received - stopping")
					processStopped = true
					return
				}
			}

		}()

	}

	normalProcess()

	time.Sleep(1 * time.Second)

	if !eventReceived {
		t.Errorf("Event not received")
	}

	if processStopped {
		t.Errorf("Process not stopped")
	}

}
