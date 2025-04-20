package gossip

import (
	"github.com/kristianJW54/GoferBroke/internal/Cluster"
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

func CreateNewDelta(keyGroup, key string, valueType ValueType, value []byte) *Cluster.Delta {

	var vt int

	switch valueType {
	case STRING:
		vt = Cluster.D_STRING_TYPE
	case BYTE:
		vt = Cluster.D_BYTE_TYPE
	case INT:
		vt = Cluster.D_INT_TYPE
	case INT64:
		vt = Cluster.D_INT64_TYPE
	case FLOAT64:
		vt = Cluster.D_FLOAT64_TYPE
	}

	return &Cluster.Delta{
		KeyGroup:  keyGroup,
		Key:       key,
		Version:   time.Now().Unix(),
		ValueType: byte(vt),
		Value:     value,
	}
}

func (n *Node) Add(d *Cluster.Delta) error {
	self := n.server.GetSelfInfo()
	return self.Store(d)
}
