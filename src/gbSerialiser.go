package src

const (
	DIGEST_TYPE = iota
	DELTA_TYPE
)

//TODO Think about the step by step flow -- need to understand the command or state of the gossip
// if it's syn-ack it will have both a digest and delta in the payload
// where is this being called and how is the serializer informed of how to handle this
// may need go-routines for processing digest whilst also processing delta
// how does this all come together in the request-response cycle with the handlers

type tmpDigest struct {
	name       string
	maxVersion int64
}

// This is still in the read-loop where the parser has called a handler for a specific command
// the handler has then needed to deSerialise in order to then carry out the command
// if needed, the server will be reached through the client struct which has the server embedded
func deSerialiseDigest(header, msg []byte) error {

	return nil
}
