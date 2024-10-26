package main

import (
	"context"
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/os_accumulator/logic"
	m "distribuidos-tp/system/os_accumulator/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName = "ID"
)

var log = logging.MustGetLogger("log")

func main() {

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("failed to create middleware: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	init_gracefull_shutdown(cancel)

	// Ejecutar el acumulador con el contexto
	osAccumulator := l.NewOSAccumulator(middleware.ReceiveGameOS, middleware.SendMetrics, middleware.SendEof)
	if err = osAccumulator.Run(ctx); err != nil {
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
