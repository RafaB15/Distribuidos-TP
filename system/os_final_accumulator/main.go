package main

import (
	"context"
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/os_final_accumulator/logic"
	m "distribuidos-tp/system/os_final_accumulator/middleware"
	"os"
	"os/signal"
	"syscall"

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	init_gracefull_shutdown(cancel)

	osAccumulator := l.NewOSFinalAccumulator(middleware.ReceiveGamesOSMetrics, middleware.SendFinalMetrics, osAccumulatorsAmount)
	if err != osAccumulator.Run(ctx) {
		log.Errorf("[ERROR]: %v", err)
		if err := middleware.Shutdown(); err != nil {
			log.Errorf("Error al cerrar el middleware: %v", err)
		}
		log.Infof("Graceful shutdown completado.")
	}

}

func init_gracefull_shutdown(cancel context.CancelFunc) {
	// Capturar señales del sistema
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)
	// Goroutine para manejar la señal y disparar la cancelación
	go func() {
		<-stopChan
		log.Info("Señal de cierre recibida. Iniciando graceful shutdown...")
		cancel()
	}()

}
