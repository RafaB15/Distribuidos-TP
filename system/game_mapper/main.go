package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/game_mapper/logic"
	m "distribuidos-tp/system/game_mapper/middleware"
	p "distribuidos-tp/system/game_mapper/persistence"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	OSAccumulatorsAmountEnvironmentVariableName      = "OS_ACCUMULATORS_AMOUNT"
	DecadeFilterAmountEnvironmentVariableName        = "DECADE_FILTER_AMOUNT"
	IndieReviewJoinersAmountEnvironmentVariableName  = "INDIE_REVIEW_JOINERS_AMOUNT"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	go u.HandlePing()
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	osAccumulatorsAmount, err := u.GetEnvInt(OSAccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	decadeFilterAmount, err := u.GetEnvInt(DecadeFilterAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	indieReviewJoinersAmount, err := u.GetEnvInt(IndieReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	actionReviewJoinersAmount, err := u.GetEnvInt(ActionReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	gameMapper := l.NewGameMapper(
		middleware.ReceiveGameBatch,
		middleware.SendGamesOS,
		middleware.SendGameYearAndAvgPtf,
		middleware.SendIndieGamesNames,
		middleware.SendActionGames,
		middleware.SendEndOfFiles,
		middleware.SendDeleteClient,
		middleware.AckLastMessages,
		log,
	)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdownWithWaitGroup(&wg, middleware, signalChannel, doneChannel, log)

	go func() {
		gameMapper.Run(
			osAccumulatorsAmount,
			decadeFilterAmount,
			indieReviewJoinersAmount,
			actionReviewJoinersAmount,
			repository,
		)
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
