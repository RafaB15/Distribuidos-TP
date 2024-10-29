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
	ActionNegativeReviewsJoinersAmountEnvironmentVariableName = "ACTION_NEGATIVE_REVIEWS_JOINERS_AMOUNT"
	NumPreviousAccumulators                                   = "NUM_PREVIOUS_ACCUMULATORS"
	FileNamePrefix                                            = "stored_reviews_"
	AccumulatedPercentileReviewsRoutingKeyPrefix              = "percentile_reviews_key_"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	actionNegativeReviewsJoinersAmount, err := u.GetEnvInt(ActionNegativeReviewsJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

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
		middleware.SendGameReviewsMetrics,
		middleware.SendEndOfFiles,
	)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		positiveReviewsFilter.Run(actionNegativeReviewsJoinersAmount, AccumulatedPercentileReviewsRoutingKeyPrefix, previousAccumulators, FileNamePrefix)
		doneChannel <- true
	}()

	<-doneChannel
}
