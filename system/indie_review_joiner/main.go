package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/indie_review_joiner/logic"
	m "distribuidos-tp/system/indie_review_joiner/middleware"
	p "distribuidos-tp/system/indie_review_joiner/persistence"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	ReviewsAccumulatorAmountEnvironmentVariableName = "REVIEWS_ACCUMULATOR_AMOUNT"
	IdEnvironmentVariableName                       = "ID"
)

var log = logging.MustGetLogger("log")

func main() {

	go u.HandlePing()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	accumulatorsAmount, err := u.GetEnvInt(ReviewsAccumulatorAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id, log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	reviewJoiner := l.NewIndieReviewJoiner(middleware.ReceiveMsg, middleware.SendMetrics, middleware.SendEof, middleware.AckLastMessage, log)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdownWithWaitGroup(&wg, middleware, signalChannel, doneChannel, log)

	go func() {
		reviewJoiner.Run(id, repository, accumulatorsAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
