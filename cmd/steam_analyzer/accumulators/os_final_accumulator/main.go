package main

import (
	"distribuidos-tp/internal/mom"

	"github.com/op/go-logging"
)

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "os_accumulator_queue"
	exchangeName       = "os_accumulator_exchange"
	queueToSendName    = "write_queue"
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	queue, err := manager.CreateQueue(queueToReceiveName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return
	}

	exchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	msgs, err := queue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
		return
	}
	forever := make(chan bool)

	go func() error {
		// Acá tenemos que recibir los mensajes de final accumulator. Una vez que recibamos tantos como nodos anteriores, mandamos al writer.
	}()

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
