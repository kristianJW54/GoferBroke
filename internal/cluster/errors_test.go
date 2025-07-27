package cluster

import (
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"testing"
)

func TestGBErrors(t *testing.T) {
	// Step 1: Create deepest GBError
	base := Errors.GossipDeferredErr

	fmt.Println("base = ", base)

	// Step 3: Chain a formatted GBError on top (ConfigChecksumFailErr)
	top := Errors.ChainGBErrorf(
		base,
		Errors.NodeNotFoundErr,
		"testing errors to unwrap",
	)

	// Step 4: Print result
	fmt.Println(top.Error())

	// Step 5: Extract and parse
	errs := Errors.ExtractGBErrors(top)
	gbErrs := Errors.UnwrapGBErrors(errs)

	for i, g := range gbErrs {
		fmt.Printf("Parsed %d: %+v\n", i, g)
	}
}

func TestGBErrorSandwichWrap(t *testing.T) {

	err := Errors.ChainGBErrorf(Errors.ResponseErr, Errors.GossipDeferredErr, "response failed after %d retries", 3)
	err2 := Errors.ChainGBErrorf(Errors.DiscoveryReqErr, err, "discovery request to %s failed", "node-5")

	wantErrCount := 3

	fmt.Println("Full Error Output:\n", err2)

	_ = Errors.HandleError(err2, func(gbErrors []*Errors.GBError) error {
		if len(gbErrors) != wantErrCount {
			return fmt.Errorf("expected %d GB errors, got %d", wantErrCount, len(gbErrors))
		}
		for _, gbErr := range gbErrors {
			fmt.Printf("gbErr: %+v\n", gbErr)
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

	fmt.Println(testErr.Error())

	errs := Errors.ExtractGBErrors(testErr)
	if len(errs) != 1 {
		t.Fatalf("expected 1 GBError, got %d", len(errs))
	}

	fmt.Printf("error extracted = %v\n", errs[0])

	finalErr := Errors.UnwrapGBErrors(errs)

	fmt.Println(finalErr[0])

	handledErr := Errors.HandleError(testErr, func(gbErrors []*Errors.GBError) error {

		for _, gbError := range gbErrors {
			if errors.Is(gbError, Errors.ConfigChecksumFailErr) {
				fmt.Printf("found the error I was looking for :)\n")
				// Do something
				return gbError
			}
		}

		return nil

	})

	fmt.Println(handledErr)

}

func TestWrapAndFmtError(t *testing.T) {
	err1 := Errors.NoRequestIDErr
	err2 := fmt.Errorf("calling from test: %w", err1)

	help := errors.Unwrap(err2)
	fmt.Println(help)

	// Use Chain, not Wrap
	err3 := Errors.ChainGBErrorf(Errors.ConfigChecksumFailErr, help, "")

	fmt.Printf("error 3 %s\n", err3.Error())

	err4 := Errors.ChainGBErrorf(Errors.NodeNotFoundErr, err3, "hello :)")

	fmt.Printf("full error = %s\n", err4)

	_ = Errors.HandleError(err4, func(chain []*Errors.GBError) error {
		for _, ge := range chain {
			fmt.Printf("found an error --> %v\n", ge)
		}
		return nil
	})
}
