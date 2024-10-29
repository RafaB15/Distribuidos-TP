package main

import (
	u "distribuidos-tp/internal/utils"
	"os"
	"os/signal"
	"syscall"

	l "distribuidos-tp/system/review_mapper/logic"
	m "distribuidos-tp/system/review_mapper/middleware"

	"github.com/op/go-logging"
)

const (
	AccumulatorsAmountEnvironmentVariableName = "ACCUMULATORS_AMOUNT"
	IdEnvironmentVariableName                 = "ID"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatorsAmount, err := u.GetEnvInt(AccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	gameMapper := l.NewReviewMapper(middleware.ReceiveGameReviews, middleware.SendReviews, middleware.SendEndOfFiles)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		gameMapper.Run(accumulatorsAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
