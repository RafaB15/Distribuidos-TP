package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/game_mapper/logic"
	m "distribuidos-tp/system/game_mapper/middleware"
	"os"
	"os/signal"
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
	)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		gameMapper.Run(
			osAccumulatorsAmount,
			decadeFilterAmount,
			indieReviewJoinersAmount,
			actionReviewJoinersAmount,
		)
		doneChannel <- true
	}()

	<-doneChannel
}
