package gossip

import (
	"bytes"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"time"
)

type Delta struct {
	KeyGroup  string
	Key       string
	Version   int64
	ValueType uint8
	Value     []byte
}

type ValueType int

const (
	STRING ValueType = iota
	BYTE
	INT
	INT64
	FLOAT64
)

// CreateNewDelta returns a new delta as a value. The delta must then be added to the nodes cluster map for it to be gossiped
func CreateNewDelta(keyGroup, key string, valueType ValueType, value []byte) Delta {

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

	return Delta{
		KeyGroup:  keyGroup,
		Key:       key,
		Version:   time.Now().Unix(),
		ValueType: byte(vt),
		Value:     value,
	}
}

// Add takes a Delta and adds it to the nodes cluster map
func (n *Node) Add(d Delta) error {

	buff := make([]byte, len(d.Value))
	copy(buff, d.Value)

	cd := &cluster.Delta{
		KeyGroup:  d.KeyGroup,
		Key:       d.Key,
		Version:   d.Version,
		ValueType: d.ValueType,
		Value:     buff,
	}

	self := n.server.GetSelfInfo()
	return self.Store(cd)
}

// Update takes a delta and tries to update it with the provided delta - if no delta is found the provided delta is stored as a new
// entry into the nodes cluster map
func (n *Node) Update(keyGroup, key string, d Delta) error {

	buff := make([]byte, len(d.Value))
	copy(buff, d.Value)

	cd := &cluster.Delta{
		KeyGroup:  d.KeyGroup,
		Key:       d.Key,
		Version:   d.Version,
		ValueType: d.ValueType,
		Value:     buff,
	}

	self := n.server.GetSelfInfo()
	err := self.Update(keyGroup, key, cd, func(toBeUpdated, by *cluster.Delta) {

		// copy fields explicitly; clone the slice
		toBeUpdated.Version = by.Version
		toBeUpdated.KeyGroup = by.KeyGroup
		toBeUpdated.Key = by.Key
		toBeUpdated.Value = bytes.Clone(by.Value)

		if keyGroup == cluster.CONFIG_DKG {
			err := n.server.UpdateClusterConfigDeltaAndSelf(key, cd)
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

func (n *Node) GetClusterName() string {
	return n.clusterConfig.Name
}
