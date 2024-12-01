package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/negative_reviews_filter/logic"
	m "distribuidos-tp/system/negative_reviews_filter/middleware"
	p "distribuidos-tp/system/negative_reviews_filter/persistence"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	EnglishReviewAccumulatorsAmountEnvironmentVariableName = "ENGLISH_REVIEW_ACCUMULATORS_AMOUNT"
	MinPositiveReviews                                     = 5000
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	englishReviewAccumulatorsAmount, err := u.GetEnvInt(EnglishReviewAccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	negativeReviewsFilter := l.NewNegativeReviewsFilter(
		middleware.ReceiveGameReviewsMetrics,
		middleware.SendQueryResults,
		middleware.AckLastMessage,
		log,
	)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdownWithWaitGroup(&wg, middleware, signalChannel, doneChannel, log)

	go func() {
		negativeReviewsFilter.Run(englishReviewAccumulatorsAmount, MinPositiveReviews, repository)
		doneChannel <- true
	}()

	<-doneChannel
}
