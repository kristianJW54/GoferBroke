package gossip

import (
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"time"
)

//type DeltaAPI interface {
//	Store(d *Cluster.Delta) error
//	Update(group, key string, d *Cluster.Delta) error
//	GetAll() map[string]Cluster.Delta
//}

type ValueType int

const (
	STRING ValueType = iota
	BYTE
	INT
	INT64
	FLOAT64
)

func CreateNewDelta(keyGroup, key string, valueType ValueType, value []byte) *cluster.Delta {

	var vt int

	switch valueType {
	case STRING:
		vt = cluster.D_STRING_TYPE
	case BYTE:
		vt = cluster.D_BYTE_TYPE
	case INT:
		vt = cluster.D_INT_TYPE
	case INT64:
		vt = cluster.D_INT64_TYPE
	case FLOAT64:
		vt = cluster.D_FLOAT64_TYPE
	}

	return &cluster.Delta{
		KeyGroup:  keyGroup,
		Key:       key,
		Version:   time.Now().Unix(),
		ValueType: byte(vt),
		Value:     value,
	}
}

func (n *Node) Add(d *cluster.Delta) error {
	self := n.server.GetSelfInfo()
	return self.Store(d)
}

// Events

func (n *Node) OnEvent(eventType cluster.EventEnum, handler func(event cluster.Event) error) (string, error) {
	return n.server.AddHandler(n.server.ServerContext, eventType, false, handler)
}
