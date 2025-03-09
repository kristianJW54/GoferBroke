package src

import (
	"errors"
	"log"
	"testing"
)

func TestGBErrors(t *testing.T) {

	gbErr := WrapGBError(ResponseErr, GossipDeferredErr)

	err := errors.New(gbErr.Error())

	log.Println(err)

	unwrapped := UnwrapError(err)

	log.Println(unwrapped[1])

}
