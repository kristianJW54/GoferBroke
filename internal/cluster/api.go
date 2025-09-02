package cluster

import "fmt"

//====================================================================
// API Functions

func (s *GBServer) GetAllParticipants() []string {

	s.clusterMapLock.RLock()
	name := s.ServerName
	parts := s.clusterMap.participantArray
	s.clusterMapLock.RUnlock()

	list := make([]string, 0, len(parts))

	for _, part := range parts {
		if part == name {
			continue
		}
		list = append(list, part)
	}

	return list

}

func (s *GBServer) GetParticipantDeltas(name string) ([]Delta, error) {

	s.clusterMapLock.RLock()
	part, exists := s.clusterMap.participants[name]
	s.clusterMapLock.RUnlock()
	if !exists {
		return nil, fmt.Errorf("no participant named %s found", name)
	}

	// Lock the participant while reading its map
	part.pm.RLock()
	defer part.pm.RUnlock()

	list := make([]Delta, 0, len(part.keyValues))
	for _, d := range part.keyValues {
		if d == nil {
			continue
		}
		nd := Delta{
			KeyGroup:  d.KeyGroup,
			Key:       d.Key,
			Version:   d.Version,
			ValueType: d.ValueType,
		}

		// Deep copy:
		if d.Value != nil {
			nd.Value = make([]byte, len(d.Value))
			copy(nd.Value, d.Value)
		}

		list = append(list, nd)
	}

	return list, nil

}

func (s *GBServer) GetDeltaFromSelf(keyGroup, key string) Delta {

	self := s.GetSelfInfo()

	self.pm.RLock()
	d, exists := self.keyValues[MakeDeltaKey(keyGroup, key)]
	self.pm.RUnlock()

	if !exists {
		return Delta{}
	}

	nd := Delta{
		KeyGroup:  d.KeyGroup,
		Key:       d.Key,
		Version:   d.Version,
		ValueType: d.ValueType,
	}

	if d.Value != nil {
		nd.Value = make([]byte, len(d.Value))
		copy(nd.Value, d.Value)
	}

	return nd

}
