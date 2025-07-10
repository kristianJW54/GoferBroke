package cluster

import (
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
	"testing"
)

func TestGBErrors(t *testing.T) {
	// Step 1: Create deepest GBError
	base := Errors.GossipDeferredErr

	log.Println("base = ", base)

	// Step 3: Chain a formatted GBError on top (ConfigChecksumFailErr)
	top := Errors.ChainGBErrorf(
		base,
		Errors.NodeNotFoundErr,
		"testing errors to unwrap",
	)

	// Step 4: Print result
	log.Println(top.Error())

	// Step 5: Extract and parse
	errs := Errors.ExtractGBErrors(top)
	gbErrs := Errors.UnwrapGBErrors(errs)

	for i, g := range gbErrs {
		log.Printf("Parsed %d: %+v", i, g)
	}
}

func TestGBErrorSandwichWrap(t *testing.T) {

	err := Errors.ChainGBErrorf(Errors.ResponseErr, Errors.GossipDeferredErr, "response failed after %d retries", 3)
	err2 := Errors.ChainGBErrorf(Errors.DiscoveryReqErr, err, "discovery request to %s failed", "node-5")

	wantErrCount := 3

	log.Println("Full Error Output:\n", err2)

	_ = Errors.HandleError(err2, func(gbErrors []*Errors.GBError) error {
		if len(gbErrors) != wantErrCount {
			return fmt.Errorf("expected %d GB errors, got %d", wantErrCount, len(gbErrors))
		}
		for _, gbErr := range gbErrors {
			log.Printf("gbErr: %+v\n", gbErr)
		}
		return nil
	})
}

func TestWrappedErrorConfigExample(t *testing.T) {
	checksum1 := "1234"
	checksum2 := "4321"
	checksum3 := "9999"

	// This is the formatted message
	message := "checksum should be: [%s] or: [%s] -- got: [%s]"

	// Create a structured GBError with formatting
	testErr := Errors.ChainGBErrorf(
		Errors.ConfigChecksumFailErr,
		nil, // no inner error, just a formatted message
		message,
		checksum1, checksum2, checksum3,
	)

	log.Println(testErr.Error())

	errs := Errors.ExtractGBErrors(testErr)
	if len(errs) != 1 {
		t.Fatalf("expected 1 GBError, got %d", len(errs))
	}

	log.Printf("error extracted = %v", errs[0])

	finalErr := Errors.UnwrapGBErrors(errs)

	log.Println(finalErr[0])

	handledErr := Errors.HandleError(testErr, func(gbErrors []*Errors.GBError) error {

		for _, gbError := range gbErrors {
			if errors.Is(gbError, Errors.ConfigChecksumFailErr) {
				log.Printf("found the error I was looking for :)")
				// Do something
				return gbError
			}
		}

		return nil

	})

	log.Println(handledErr)

}

func TestWrapAndFmtError(t *testing.T) {

	err1 := Errors.NoRequestIDErr

	err2 := fmt.Errorf("calling from test: %w", err1)

	err3 := Errors.WrapGBError(Errors.GossipDeferredErr, err2)

	// Can also use Chain here - maybe should standardise to use chain and remove Wrap??
	//err3 := Errors.ChainGBErrorf(Errors.GossipDeferredErr, err2, "")

	err4 := Errors.ChainGBErrorf(Errors.NodeNotFoundErr, err3, "hello :)")

	_ = Errors.HandleError(err4, func(gbErrors []*Errors.GBError) error {

		for _, gbError := range gbErrors {
			log.Printf("found an error --> %v", gbError)
		}

		return nil

	})

}
