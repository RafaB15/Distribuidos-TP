package main

import (
	l "distribuidos-tp/system/decade_filter/logic"
	m "distribuidos-tp/system/decade_filter/middleware"

	"github.com/op/go-logging"
)

const (
	DecadeFiltersAmountEnvironmentVariableName = "DECADE_FILTERS_AMOUNT"
	FileName                                   = "top_ten_games"
)

var log = logging.MustGetLogger("log")

func main() {

	log.Infof("Starting Decade Filter")
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	decadeFilter := l.NewDecadeFilter(middleware.ReceiveYearAvgPtf, middleware.SendFilteredYearAvgPtf, middleware.SendEof)
	decadeFilter.Run()
}
