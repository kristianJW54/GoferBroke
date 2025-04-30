package cluster

import (
	"context"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
)

type EventErrorType int

const (
	TestError EventErrorType = iota + 1
	ConnectToSeed
	NewJoinError
	SendSelfError
)

type EventErrorSeverity int

const (
	CollectAndAct EventErrorSeverity = iota + 1
	Warn
	Recoverable
	Critical
)

type errorController struct {
	s *GBServer
}

type errorContext struct {
	ctx           context.Context
	controller    ErrorController
	dispatchEvent func(Event) // Gives us the ability to chain event errors and escalate
}

type ErrorController interface {
	Shutdown()
}

func (ec *errorController) Shutdown() {
	ec.s.Shutdown()
}

//=======================================================
// Internal Event Handlers + Event Registers
//=======================================================

func handleInternalError(ctx *errorContext, e Event) error {

	event, ok := e.Payload.(*ErrorEvent)
	if !ok {
		return Errors.InternalErrorHandlerErr
	}

	switch event.ErrorType {

	case TestError:
		if err := processTestError(ctx, event, e.Time, e.Message); err != nil {
			return err
		}
	case ConnectToSeed:
		if err := processConnectToSeedError(ctx, event, e.Time, e.Message); err != nil {
			return err
		}
	default:
		return nil
	}

	return nil

}

func processTestError(ctx *errorContext, e *ErrorEvent, time int64, message string) error {

	switch e.Severity {

	case CollectAndAct:
		log.Println("got test error with a severity of CollectAndAct")
		return nil
	default:
		return nil
	}

}

func processConnectToSeedError(ctx *errorContext, e *ErrorEvent, time int64, message string) error {

	switch e.Severity {

	case CollectAndAct:
		log.Println("got test error with a severity of CollectAndAct")
		return nil
	case Critical:
		log.Printf("Critical error: %s", message)
		ctx.controller.Shutdown()
		return nil
	default:
		return nil
	}

}
