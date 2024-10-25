package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/top_ten_accumulator/logic"
	m "distribuidos-tp/system/top_ten_accumulator/middleware"

	"github.com/op/go-logging"
)

const (
	DecadeFiltersAmountEnvironmentVariableName = "DECADE_FILTERS_AMOUNT"
	FileName                                   = "top_ten_games"
)

var log = logging.MustGetLogger("log")

func main() {
	log.Infof("Starting Top Ten Accumulator")

	filtersAmount, err := u.GetEnvInt(DecadeFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	topTenAccumulator := l.NewTopTenAccumulator(middleware.ReceiveMsg, middleware.SendMsg)
	topTenAccumulator.Run(filtersAmount, FileName)
}
