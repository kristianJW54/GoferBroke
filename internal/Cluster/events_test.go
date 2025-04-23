package Cluster

import (
	"context"
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
