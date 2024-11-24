package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/percentile_accumulator/logic"
	m "distribuidos-tp/system/percentile_accumulator/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	NumPreviousAccumulators = "NUM_PREVIOUS_ACCUMULATORS"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	previousAccumulators, err := u.GetEnvInt(NumPreviousAccumulators)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	log.Info("Starting Percentile Accumulator")
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	positiveReviewsFilter := l.NewPercentileAccumulator(
		middleware.ReceiveGameReviewsMetrics,
		middleware.SendQueryResults,
	)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		positiveReviewsFilter.Run(previousAccumulators)
		doneChannel <- true
	}()

	<-doneChannel
}
