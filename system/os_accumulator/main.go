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

	// Crear un contexto con cancelación para manejar el shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capturar señales del sistema
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	osAccumulator := l.NewOSAccumulator(middleware.ReceiveGameOS, middleware.SendMetrics, middleware.SendEof)

	// Goroutine para manejar la señal y disparar la cancelación
	go func() {
		<-stopChan
		log.Info("Señal de cierre recibida. Iniciando graceful shutdown...")
		cancel()
	}()

	// Ejecutar el acumulador con el contexto
	osAccumulator.Run(ctx)

	// Cerrar el middleware y otros recursos
	if err := middleware.Shutdown(); err != nil {
		log.Errorf("Error al cerrar el middleware: %v", err)
	}
	log.Info("Graceful shutdown completado.")
}
