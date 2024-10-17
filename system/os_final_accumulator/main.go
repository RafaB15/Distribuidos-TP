package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/os_final_accumulator/logic"
	m "distribuidos-tp/system/os_final_accumulator/middleware"

	"github.com/op/go-logging"
)

const OSAccumulatorsAmountEnvironmentVariableName = "NUM_PREVIOUS_OS_ACCUMULATORS"

var log = logging.MustGetLogger("log")

func main() {

	osAccumulatorsAmount, err := u.GetEnvInt(OSAccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	osAccumulator := l.NewOSFinalAccumulator(middleware.ReceiveGamesOSMetrics, middleware.SendFinalMetrics, osAccumulatorsAmount)
	osAccumulator.Run()
}
