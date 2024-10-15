package main

import (
	l "distribuidos-tp/system/os_accumulator/logic"
	m "distribuidos-tp/system/os_accumulator/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	osAccumulator := l.NewOSAccumulator(middleware.ReceiveGameOS, middleware.SendMetrics)
	osAccumulator.Run()
}
