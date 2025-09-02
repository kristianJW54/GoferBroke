package cluster

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"log/slog"
	"sync"
)

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

//---------------------------------------------------
// Delta Updated

type DeltaUpdateEvent struct {
	DeltaGroup      string
	DeltaKey        string
	PreviousVersion int64
	PreviousValue   []byte
	CurrentVersion  int64
	CurrentValue    []byte
}

// Do not need these internal handlers

//---------------------------------------------------
// Delta Added

type DeltaAddedEvent struct {
	DeltaGroup string
	DeltaKey   string
	DeltaValue []byte
}

// Do not need these internal handlers

func (ed *EventDispatcher) HandleDeltaAddedEvent(e Event) error {

	payload, ok := e.Payload.(*DeltaAddedEvent)
	if !ok {
		return fmt.Errorf("invalid payload for DeltaUpdateEvent")
	}

	fmt.Printf("Delta %s added: %v", payload.DeltaKey, payload.DeltaValue)

	return nil

}

//---------------------------------------------------
// New Node Join

type NewNodeJoin struct {
	Name    string
	Time    int64
	Address string
}

//---------------------------------------------------
// New Participant Added

type NewParticipantJoin struct {
	Name       string
	Time       int64
	MaxVersion int64
}

//---------------------------------------------------
// Participant declared dead

type ParticipantFaulty struct {
	Name    string
	Time    int64
	Address string
}
