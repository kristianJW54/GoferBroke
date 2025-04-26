package cluster

import (
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
)

//=======================================================
// Internal Event Handlers + Event Registers
//=======================================================

func handleInternalError(e Event) error {

	event, ok := e.Payload.(*ErrorEvent)
	if !ok {
		return Errors.InternalErrorHandlerErr
	}

	switch event.ErrorType {

	case TestError:
		if err := processTestError(event); err != nil {
			return err
		}
	default:
		return nil
	}

	return nil

}

func processTestError(e *ErrorEvent) error {

	switch e.Severity {

	case CollectAndAct:
		log.Println("got test error with a severity of CollectAndAct")
		return nil
	default:
		return nil
	}

}
