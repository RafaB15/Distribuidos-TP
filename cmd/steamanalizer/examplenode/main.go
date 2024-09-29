package main

import (
	"distribuidos-tp/internal/mom"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"
	exchangeName  = "game_exchange"
	exchangeType  = "direct"
	queueName     = "game_queue"
	routingKey    = "game_key"
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI, 5, 2)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	queue, err := manager.CreateQueue(queueName, exchangeName, routingKey)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	msgs, err := queue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Info("Received a message: %s\n", d.Body)
		}
	}()

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
