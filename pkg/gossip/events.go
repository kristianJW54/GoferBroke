package gossip

import (
	"github.com/google/uuid"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
)

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
)

type Event interface {
	Type() EventEnum
	Time() int64
	Payload() any
	Message() string
}

type internalEventMapping struct {
	eventType EventEnum
	time      int64
	payload   any
	message   string
}

func (i *internalEventMapping) Type() EventEnum {
	return i.eventType
}

func (i *internalEventMapping) Time() int64 {
	return i.time
}

func (i *internalEventMapping) Payload() any {
	return i.payload
}

func (i *internalEventMapping) Message() string {
	return i.message
}

func mapInternalEventTypeToPublic(internalType cluster.EventEnum) EventEnum {
	switch internalType {
	case cluster.NewDeltaAdded:
		return NewDeltaAdded
	case cluster.DeltaUpdated:
		return DeltaUpdated
	case cluster.NewParticipantAdded:
		return NewParticipantAdded
	// TODO Add all safe mappings
	default:
		return 0 // <-- Define UnknownEvent EventEnum
	}
}

func mapToPublicEvent(e cluster.Event) Event {
	return &internalEventMapping{
		eventType: mapInternalEventTypeToPublic(e.EventType),
		time:      e.Time,
		payload:   mapInternalPayloadToPublic(e.EventType, e.Payload),
		message:   e.Message,
	}
}

func mapInternalPayloadToPublic(eventType cluster.EventEnum, payload any) any {
	switch eventType {
	case cluster.NewDeltaAdded:
		if internal, ok := payload.(*cluster.DeltaAddedEvent); ok {
			return &DeltaAddedEvent{ // <-- public SDK struct
				DeltaGroup: internal.DeltaGroup,
				DeltaKey:   internal.DeltaKey,
				DeltaValue: internal.DeltaValue,
			}
		}
	case cluster.DeltaUpdated:
		if internal, ok := payload.(*cluster.DeltaUpdateEvent); ok {
			return &DeltaUpdateEvent{
				DeltaGroup:      internal.DeltaGroup,
				DeltaKey:        internal.DeltaKey,
				PreviousVersion: internal.PreviousVersion,
				PreviousValue:   internal.PreviousValue,
				CurrentVersion:  internal.CurrentVersion,
				CurrentValue:    internal.CurrentValue,
			}
		}
	case cluster.NewParticipantAdded:
		if internal, ok := payload.(*cluster.NewParticipantJoin); ok {
			return &NewParticipantJoin{
				Name:       internal.Name,
				Time:       internal.Time,
				MaxVersion: internal.MaxVersion,
			}
		}
		// Handle other types...
	}
	return payload // fallback if unknown
}

func (n *Node) OnEvent(eventType EventEnum, handler func(Event) error) (string, error) {
	id := uuid.New().String()

	registrationFn := func(s *cluster.GBServer) error {
		internalHandler := func(e cluster.Event) error {
			sdkEvent := mapToPublicEvent(e)
			return handler(sdkEvent)
		}

		// Register it using the actual server
		_, err := s.AddHandler(s.ServerContext, cluster.EventEnum(eventType), false, internalHandler)
		return err
	}

	// Add to pending handlers so it gets registered during StartServer
	n.server.AddHandlerRegistration(registrationFn)

	return id, nil
}

//=======================================================
// Event Structs & Types
//=======================================================

type DeltaUpdateEvent struct {
	DeltaGroup      string
	DeltaKey        string
	PreviousVersion int64
	PreviousValue   []byte
	CurrentVersion  int64
	CurrentValue    []byte
}

type DeltaAddedEvent struct {
	DeltaGroup string
	DeltaKey   string
	DeltaValue []byte
}

type NewNodeJoin struct {
	Name    string
	Time    int64
	Address string
}

type NewParticipantJoin struct {
	Name       string
	Time       int64
	MaxVersion int64
}
