package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/final_english_joiner/logic"
	m "distribuidos-tp/system/final_english_joiner/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	ActionEnglishJoinersAmountEnvironmentVariableName = "ACTION_ENGLISH_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	actionEnglishJoinersAmount, err := u.GetEnvInt(ActionEnglishJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	finalEnglishJoiner := l.NewFinalPositiveJoiner(middleware.ReceiveJoinedGameReviews, middleware.SendQueryResults)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		finalEnglishJoiner.Run(actionEnglishJoinersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
