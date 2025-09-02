package gossip

import (
	"bytes"
	"fmt"
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

func (n *Node) Update(keyGroup, key string, d *cluster.Delta) error {
	self := n.server.GetSelfInfo()
	err := self.Update(keyGroup, key, d, func(toBeUpdated, by *cluster.Delta) {

		// copy fields explicitly; clone the slice
		toBeUpdated.Version = by.Version
		toBeUpdated.KeyGroup = by.KeyGroup
		toBeUpdated.Key = by.Key
		toBeUpdated.Value = bytes.Clone(by.Value)

		if keyGroup == cluster.CONFIG_DKG {
			err := n.server.UpdateClusterConfigDeltaAndSelf(key, d)
			if err != nil {
				return
			}
		}

	})
	return err
}

func (n *Node) GetAllParticipants() []string {
	return n.server.GetAllParticipants()
}

func (n *Node) GetParticipantDeltas(name string) ([]Delta, error) {

	d, err := n.server.GetParticipantDeltas(name)
	if err != nil {
		return nil, fmt.Errorf("error getting deltas for participant %s: %s", name, err)
	}

	out := make([]Delta, len(d))
	for i, delta := range d {

		nd := Delta{
			KeyGroup:  delta.KeyGroup,
			Key:       delta.Key,
			Version:   delta.Version,
			ValueType: delta.ValueType,
		}

		if delta.Value != nil {
			nd.Value = make([]byte, len(delta.Value))
			copy(nd.Value, delta.Value)
		}
		out[i] = nd
	}

	return out, nil

}

func (n *Node) GetDeltaFromSelf(keyGroup, key string) Delta {

	delta := n.server.GetDeltaFromSelf(keyGroup, key)
	nd := Delta{
		KeyGroup:  delta.KeyGroup,
		Key:       delta.Key,
		Version:   delta.Version,
		ValueType: delta.ValueType,
	}

	if delta.Value != nil {
		nd.Value = make([]byte, len(delta.Value))
		copy(nd.Value, delta.Value)
	}

	return nd

}
