package cluster

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"log/slog"
	"sync"
)

/*

Event Strategy — Reviewed

There are two types of events in the system: **Internal** and **External**

---

🔹 Internal Events

Internal events are **system-level events** used to monitor or react to internal mechanics of the gossip protocol. These events are:
- **Pre-registered** by the system
- **Handled by the core** (not user-defined logic)
- Used to track or enforce protocol behavior
- Triggered only by **internal deltas** or system operations

These events are **strictly read-only for the server's self state**. That means:
- Even if a peer gossips an internal delta about "you", the server will not merge that value into its **own** state
- Instead, it will trigger an internal event for observation or failure detection

Example:
`InternalDeltaUpdated` — triggered when a node receives a newer version of an internal delta (e.g., number of connections) about a peer.

**Why this matters:**
> Internal deltas must never overwrite or influence the server’s self-state, even if other nodes gossip conflicting information.

---

🔸 External Events

External events are triggered by **user-defined or application-level deltas** and changes in the system. These events:
- Can be **reacted to freely** by application developers
- May lead to updates in the server’s own state, if the application chooses
- Are **loosely coupled** to the protocol — acting on them does not affect core correctness

These events give **application designers flexibility** to use the gossip system as a state dissemination mechanism.

Example:
`UserDeltaReceived` — triggered when a node receives a new delta about a "to-do list" key from another peer.

> Application logic may choose to incorporate the peer’s state into its own (e.g., merging tasks), but this is optional and application-specific.

---

🛑 Warning on Merging External State

While external deltas **can** be merged into the server’s self state, it is **not recommended by default**. This blurs the ownership model and can lead to:
- Diverging update responsibilities
- Unclear authority over data

Applications must make **explicit decisions** when choosing to mirror peer state.

---

The event system provides a simple, reactive way to surface what’s happening inside the cluster.
It gives users visibility into key changes and lets them hook into events for things like logging, metrics, dashboards, or forwarding to external tools (e.g., HTTP tracing endpoints).
It’s designed to stay out of the core gossip logic while offering enough flexibility for users to monitor and respond to state changes as needed.

*/

// Need a system or gossip observer which we can register to watch certain events
// the observer then listens on channels - sends them to waiting handlers to handle
// if no handler then move on

// EventEnum will tell the event handler what event type is being handled and will allow the handler to then handle that event
// the internalEvent struct will have extra information which the handler can use depending on the event type
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

	AdvertiseAddressUpdatedFromDial

	InternalError

	// Will need error events to provide the option to handle in-flight errors
	// Also register system(internal) error handlers to handle

)

type HandlerRegistrationFunc func(s *GBServer) error

func (s *GBServer) AddHandlerRegistration(fn HandlerRegistrationFunc) {
	s.pendingHandlerRegs = append(s.pendingHandlerRegs, fn)
}

type Event struct {
	EventType EventEnum
	Time      int64
	Payload   any    // Event payload interpreted and decoded by handler based on event type
	Message   string // Human readable event reasoning
}

type handlerEvent struct {
	id         string
	eventCh    chan Event
	handler    func(Event) error
	isInternal bool
}

// EventDispatcher is a simple struct which will dispatch incoming events to registered handlers
type EventDispatcher struct {
	mu       *sync.Mutex
	handlers map[EventEnum][]*handlerEvent
}

//------------------------
// Event Enum Parser

func ParseEventEnumToString(EventType EventEnum) string {

	switch EventType {
	case NewDeltaAdded:
		return "New Delta Added"
	case DeltaUpdated:
		return "Delta Updated"
	case NewParticipantAdded:
		return "New Participant Added"
	case ParticipantUpdated:
		return "Participant Updated"
	case ParticipantMarkedDead:
		return "Participant Marked Dead"
	case ReceivedNewDialFromNode:
		return "Received New Dial From Node"
	case WatchedDeltaUpdated:
		return "Watched Delta Updated"
	case WatchedDeltaGroupUpdated:
		return "Watched Delta Group Updated"
	case WatchedDeltaGroupDeltaAdded:
		return "Watched Delta Group Delta Added"
	case ClientConnected:
		return "Client Connected"
	case ClientDisconnected:
		return "Client Disconnected"
	case GossipLoadReached:
		return "Gossip Load Reached"
	case MaxNodeConnectionsReached:
		return "Max Connections Reached"
	case MaxClientConnectionsReached:
		return "Max Connections Reached"
	case AdvertiseAddressUpdatedFromDial:
		return "Advertise Address Updated From Dial"
	case InternalError:
		return "Internal Error"
	default:
		return ""
	}
}

//------------------------
// Event Dispatcher

// NewEventDispatcher will create a global handler holder embedded in the server add event handlers to
func NewEventDispatcher() *EventDispatcher {

	return &EventDispatcher{
		mu:       &sync.Mutex{},
		handlers: make(map[EventEnum][]*handlerEvent),
	}

}

// AddHandler registers a new handler with the EventDispatcher and returns an id which can be used to access the event in the handler
// to de-register if needed
func (s *GBServer) AddHandler(ctx context.Context, eventType EventEnum, isInternal bool, handler func(Event) error) (string, error) {
	id := uuid.New().String()
	ch := make(chan Event, 128)
	entry := &handlerEvent{
		id:         id,
		eventCh:    ch,
		handler:    handler,
		isInternal: isInternal,
	}

	s.event.mu.Lock()
	s.event.handlers[eventType] = append(s.event.handlers[eventType], entry)
	s.event.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(1)

	// Launch go routine for reading events on channel
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Warn("panic in event handler", slog.String("event_type", ParseEventEnumToString(eventType)))
			}
		}()

		wg.Done() // signal ready

		for {
			select {
			case event := <-ch:
				if err := handler(event); err != nil {
					s.logger.Error("error handling event", slog.String("event_type", ParseEventEnumToString(eventType)), slog.String("error", err.Error()))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait() // block until handler loop is running

	return id, nil
}

// Internal handler for registering internal events within the system NOT exposed to public API
func (s *GBServer) addInternalHandler(ctx context.Context, eventType EventEnum, handler func(Event) error) (string, error) {
	return s.AddHandler(ctx, eventType, true, handler)
}

// DispatchEvent will loop through the registered handlers and send the incoming event out - fan out
func (s *GBServer) DispatchEvent(event Event) {

	s.event.mu.Lock()
	defer s.event.mu.Unlock()

	if registered, exists := s.event.handlers[event.EventType]; exists {

		for _, handler := range registered {

			select {
			case handler.eventCh <- event:
				//Delivered
			default:
				s.logger.Warn("event dropped",
					slog.String("event type", ParseEventEnumToString(event.EventType)),
					slog.Int64("event time", event.Time),
				)
			}

		}

	}

}

//---------------------------
// Error events - INTERNAL

type ErrorEvent struct {
	ErrorType EventErrorType
	Severity  EventErrorSeverity
	Error     error
	Caller    string
}

//=======================================================
// Event Structs & Types
//=======================================================

type DeltaUpdateEvent struct {
	DeltaKey        string
	PreviousVersion int64
	PreviousValue   []byte
	CurrentVersion  int64
	CurrentValue    []byte
}

func (ed *EventDispatcher) HandleDeltaUpdateEvent(e Event) error {

	payload, ok := e.Payload.(*DeltaUpdateEvent)
	if !ok {
		return fmt.Errorf("invalid payload for DeltaUpdateEvent")
	}

	fmt.Printf("Delta %s updated: \nPrevious value: %s\nNew value: %s\n", payload.DeltaKey, payload.PreviousValue, payload.CurrentValue)

	return nil

}

type DeltaAddedEvent struct {
	DeltaKey   string
	DeltaValue []byte
}

func (ed *EventDispatcher) HandleDeltaAddedEvent(e Event) error {

	payload, ok := e.Payload.(*DeltaAddedEvent)
	if !ok {
		return fmt.Errorf("invalid payload for DeltaUpdateEvent")
	}

	fmt.Printf("Delta %s added: %v", payload.DeltaKey, payload.DeltaValue)

	return nil

}

type NewNodeJoin struct {
	Name    string
	Time    int64
	Address string
}
