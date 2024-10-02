package main

import (
	"distribuidos-tp/internal/mom"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"encoding/csv"
	"errors"
	"strings"

	"github.com/op/go-logging"
)

const (
	middlewareURI   = "amqp://guest:guest@rabbitmq:5672/"
	queueName       = "game_queue"
	queueToSendName = "os_game_queue"
	exchangeName    = "os_game_exchange"
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

	game_os_queue, err := manager.CreateQueue(queueToSendName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	game_os_exchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	err = game_os_queue.Bind(game_os_exchange.Name, "os")

	forever := make(chan bool)

	go mapLines(queue, game_os_exchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapLines(queue *mom.Queue, game_os_exchange *mom.Exchange) error {
	msgs, err := queue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}
	for d := range msgs {
		var input string = string(d.Body)

		reader := csv.NewReader(strings.NewReader(input))
		records, err := reader.Read()
		if err != nil {
			return err
		}

		if len(records) < 20 {
			return errors.New("input CSV does not have enough fields")
		}

		gameOs, err := oa.NewGameOs(records[0], records[17], records[18], records[19])
		if err != nil {
			return err
		}

		serializedGameOs := oa.SerializeGameOs(gameOs)
		err = game_os_exchange.Publish("os", serializedGameOs)
		if err != nil {
			log.Error("Error publishing game")
			return err
		}

		log.Infof("Received a message (after attempted send): %s\n", string(d.Body))
	}

	return nil
}
