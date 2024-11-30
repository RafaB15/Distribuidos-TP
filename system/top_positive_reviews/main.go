package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/top_positive_reviews/logic"
	m "distribuidos-tp/system/top_positive_reviews/middleware"
	p "distribuidos-tp/system/top_positive_reviews/persistence"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IndieReviewJoinersAmountEnvironmentVariableName = "INDIE_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {

	go handlePing()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	indieReviewJoinersAmount, err := u.GetEnvInt(IndieReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	topPositiveReviews := l.NewTopPositiveReviews(middleware.ReceiveMsg, middleware.SendQueryResults, middleware.AckLastMessage, log)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdownWithWaitGroup(&wg, middleware, signalChannel, doneChannel, log)

	go func() {
		topPositiveReviews.Run(indieReviewJoinersAmount, repository)
		doneChannel <- true
	}()

	<-doneChannel
}

func handlePing() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// fmt.Fprintln(w, "Pong")
	})

	if err := http.ListenAndServe(":80", nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}
