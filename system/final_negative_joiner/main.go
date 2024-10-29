package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/final_negative_joiner/logic"
	m "distribuidos-tp/system/final_negative_joiner/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	ActionNegativeJoinersAmountEnvironmentVariableName = "ACTION_NEGATIVE_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	actionNegativeJoinersAmount, err := u.GetEnvInt(ActionNegativeJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	finalNegativeJoiner := l.NewFinalNegativeJoiner(middleware.ReceiveJoinedGameReviews, middleware.SendQueryResults)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		finalNegativeJoiner.Run(actionNegativeJoinersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
