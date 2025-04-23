package Cluster

import "sync"

// Need a system or gossip observer which we can register to watch certain events
// the observer then listens on channels - sends them to waiting handlers to handle
// if no handler then move on

// EventEnum will tell the event handler what event type is being handled and will allow the handler to then handle that event
// the Event struct will have extra information which the handler can use depending on the event type
type EventEnum int

const (
	NewDeltaAdded EventEnum = iota + 1
	DeltaUpdated
	NewParticipantAdded
	ParticipantUpdated
	ParticipantMarkedDead
	ReceivedNewDialFromNode
	WatchedDeltaUpdated
	WatchedDeltaGroupUpdated
	WatchedDeltaGroupDeltaAdded

	ClientConnected
	ClientDisconnected

	GossipLoadReached
	MaxNodeConnectionsReached
	MaxClientConnectionsReached
)

type Event struct {
	EventType EventEnum
	Time      int64
	Payload   any    // Event payload interpreted and decoded by handler based on event type
	Message   string // Human readable event reasoning
}

type handlerEvent struct {
	id         string
	eventCh    chan Event
	handler    func(Event)
	isInternal bool
}

// EventDispatcher is a simple struct which will dispatch incoming events to registered handlers
type EventDispatcher struct {
	mu       *sync.Mutex
	handlers map[EventEnum][]*handlerEvent
}

//=======================================================
// Event Structs & Types
//=======================================================

// Handler Register

//TODO We should have internal registered handlers IF there are endpoints registered such as monitoring or logging URLS
// Also should have lightweight handlers which can signal critical errors which we may want to react to
