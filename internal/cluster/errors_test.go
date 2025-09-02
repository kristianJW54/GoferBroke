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

	// Step 3: Chain a formatted GBError on top (ConfigChecksumFailErr)
	top := Errors.ChainGBErrorf(
		base,
		Errors.NodeNotFoundErr,
		"testing errors to unwrap",
	)

	// Step 5: Extract and parse
	errs := Errors.ExtractGBErrors(top)
	gbErrs := Errors.UnwrapGBErrors(errs)

	if gbErrs[0].Code != 11 {
		t.Errorf("expected code 11 got %d", gbErrs[0].Code)
	}
	if gbErrs[1].Code != 63 {
		t.Errorf("expected code 63 got %d", gbErrs[1].Code)
	}
}

func TestGBErrorSandwichWrap(t *testing.T) {

	err := Errors.ChainGBErrorf(Errors.ResponseErr, Errors.GossipDeferredErr, "response failed after %d retries", 3)
	err2 := Errors.ChainGBErrorf(Errors.DiscoveryReqErr, err, "discovery request to %s failed", "node-5")

	wantErrCount := 3

	_ = Errors.HandleError(err2, func(gbErrors []*Errors.GBError) error {
		if len(gbErrors) != wantErrCount {
			return fmt.Errorf("expected %d GB errors, got %d", wantErrCount, len(gbErrors))
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

	errs := Errors.ExtractGBErrors(testErr)
	if len(errs) != 1 {
		t.Fatalf("expected 1 GBError, got %d", len(errs))
	}

	he := Errors.HandleError(testErr, func(gbErrors []*Errors.GBError) error {

		for _, gbError := range gbErrors {
			if gbError.Code != 20 {
				// Do something
				t.Errorf("expected code 20 got %d", gbError.Code)
				return gbError
			}
		}

		return nil

	})

	if he != nil {
		t.Errorf("expected error to be nil")
	}

}

func TestWrapAndFmtError(t *testing.T) {
	err1 := Errors.NoRequestIDErr
	err2 := fmt.Errorf("calling from test: %w", err1)

	help := errors.Unwrap(err2)

	// Use Chain, not Wrap
	err3 := Errors.ChainGBErrorf(Errors.ConfigChecksumFailErr, help, "")

	unwrap := 0

	err4 := Errors.ChainGBErrorf(Errors.NodeNotFoundErr, err3, "hello :)")

	_ = Errors.HandleError(err4, func(chain []*Errors.GBError) error {
		for _, _ = range chain {
			unwrap++
		}
		return nil
	})

	if unwrap != 3 {
		t.Errorf("expected 3 errors, got %d", unwrap)
	}

}
