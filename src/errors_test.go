package src

import (
	"fmt"
	"log"
	"testing"
)

func TestGBErrors(t *testing.T) {

	gbErr := WrapGBError(ResponseErr, GossipDeferredErr)

	err := fmt.Errorf("testing error string to unwrap - %w", gbErr)

	unwrapped := UnwrapError(err)

	log.Println(unwrapped[1])

}

func TestGBErrorSandwichWrap(t *testing.T) {

	err := fmt.Errorf("testing error string to unwrap - %w", GossipDeferredErr)

	err2 := WrapGBError(ResponseErr, err)

	err3 := fmt.Errorf("big boi nesting - %w", err2)

	err4 := WrapGBError(DiscoveryReqErr, err3)

	log.Println(err4)

	errs := UnwrapError(err4)

	log.Println(errs[2])

}
