package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/game_mapper/logic"
	m "distribuidos-tp/system/game_mapper/middleware"

	"github.com/op/go-logging"
)

const (
	OSAcumulatorsAmountEnvironmentVariableName       = "OS_ACCUMULATORS_AMOUNT"
	DecadeFilterAmountEnvironmentVariableName        = "DECADE_FILTER_AMOUNT"
	IndieReviewJoinersAmountEnvironmentVariableName  = "INDIE_REVIEW_JOINERS_AMOUNT"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	osAccumulatorsAmount, err := u.GetEnvInt(OSAcumulatorsAmountEnvironmentVariableName)
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

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	gameMapper := l.NewGameMapper(
		middleware.ReceiveGameBatch,
		middleware.SendGamesOS,
		middleware.SendGameYearAndAvgPtf,
		middleware.SendIndieGamesNames,
		middleware.SendActionGamesNames,
		middleware.SendEndOfFiles,
	)

	gameMapper.Run(
		osAccumulatorsAmount,
		decadeFilterAmount,
		indieReviewJoinersAmount,
		actionReviewJoinersAmount,
	)
}
