package main

import (
	"distribuidos-tp/internal/mom"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"
	queueName     = "os_game_queue"
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	queue, err := manager.CreateQueue(queueName)
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
			gameOs, err := oa.DeserializeGameOS(d.Body)
			if err != nil {
				log.Error("Error deserializing GameOS")
			}

			log.Debugf("Got game of id %d", gameOs.AppId)
		}
	}()

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
