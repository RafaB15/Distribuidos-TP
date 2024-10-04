package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
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
		log.Debugf("Printing fields: 15 : %v, 16 : %v, 17 : %v, 18 : %v, 19 : %v, 20 : %v", records[15], records[16], records[17], records[18], records[19], records[20])
		gameOs, err := oa.NewGameOS(records[19], records[18], records[17])
		if err != nil {
			log.Error("Hubo errorcito")
			return err
		}
		gameOsSlice := []*oa.GameOS{gameOs}
		serializedGameOS := sp.SerializeMsgGameOSInformation(gameOsSlice)

		err = game_os_exchange.Publish("os", serializedGameOS)
		if err != nil {
			log.Error("Error publishing game")
			return err
		}

		// log.Infof("Received a message (after attempted send): %s\n", string(d.Body))
	}

	return nil
}
