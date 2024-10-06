package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"encoding/csv"
	"strings"

	"github.com/op/go-logging"
)

const (
	middlewareURI   = "amqp://guest:guest@rabbitmq:5672/"
	queueName       = "game_queue"
	queueToSendName = "os_game_queue"
	exchangeName    = "os_game_exchange"
	numNextNodes    = 2
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

	gameOSQueue, err := manager.CreateQueue(queueToSendName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	gameOSExchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	err = gameOSQueue.Bind(gameOSExchange.Name, "os")

	if err != nil {
		log.Errorf("Failed to bind accumulator queue: %v", err)
		return
	}

	forever := make(chan bool)

	go mapLines(queue, gameOSExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapLines(queue *mom.Queue, gameOSExchange *mom.Exchange) error {
	msgs, err := queue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}
	for d := range msgs {

		msgType, err := sp.DeserializeMessageType(d.Body)
		if err != nil {
			return err
		}

		switch msgType {

		case sp.MsgEndOfFile:

			for i := 0; i < numNextNodes; i++ {
				gameOSExchange.Publish("os", sp.SerializeMsgEndOfFile())
			}

			log.Info("End of file received")
		case sp.MsgBatch:
			input, err := sp.DeserializeBatchMsg(d.Body)
			if err != nil {
				return err
			}
			reader := csv.NewReader(strings.NewReader(input))
			records, err := reader.Read()
			if err != nil {
				return err
			}
			log.Debugf("Printing fields: 0 : %v, 1 : %v, 2 : %v, 3 : %v, 4 : %v", records[0], records[1], records[2], records[3], records[4])
			gameOs, err := oa.NewGameOS(records[2], records[3], records[4])
			if err != nil {
				log.Error("Hubo errorcito")
				return err
			}
			// En realidad se deberían mandar muchos juegos por mensajes
			gameOsSlice := []*oa.GameOS{gameOs}
			serializedGameOS := sp.SerializeMsgGameOSInformation(gameOsSlice)

			err = gameOSExchange.Publish("os", serializedGameOS)
			if err != nil {
				log.Error("Error publishing game")
				return err
			}

			log.Infof("Received a message (after attempted send): %s\n", string(d.Body))

		}

	}

	return nil
}
