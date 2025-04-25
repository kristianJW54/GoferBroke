package cluster

import (
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
	"testing"
)

func TestGBErrors(t *testing.T) {

	gbErr := Errors.WrapGBError(Errors.ResponseErr, Errors.GossipDeferredErr)

	err := fmt.Errorf("testing error string to unwrap - %w", gbErr)

	errs := Errors.ExtractGBErrors(err)

	gbErrs := Errors.UnwrapGBErrors(errs)
	log.Println(gbErrs[1])

}

func TestGBErrorSandwichWrap(t *testing.T) {

	err := fmt.Errorf("testing error string to unwrap - %w", Errors.GossipDeferredErr)

	err2 := Errors.WrapGBError(Errors.ResponseErr, err)

	err3 := fmt.Errorf("big boi nesting - %w", err2)

	err4 := Errors.WrapGBError(Errors.DiscoveryReqErr, err3)

	log.Println(err4)

	errs := Errors.ExtractGBErrors(err4)

	log.Println(errs[2])

}
