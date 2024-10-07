package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	"encoding/csv"
	"io"
	"strings"

	"github.com/op/go-logging"
)

const (
	middlewareURI             = "amqp://guest:guest@rabbitmq:5672/"
	queueName                 = "game_queue"
	osQueueName               = "os_game_queue"
	yearAndAvgPtfQueueName    = "year_and_avg_ptf_queue"
	osExchangeName            = "os_game_exchange"
	yearAndAvgPtfExchangeName = "year_and_avg_ptf_exchange"
	numNextNodes              = 2
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

	//
	gameOSQueue, err := manager.CreateQueue(osQueueName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return
	}

	gameOSExchange, err := manager.CreateExchange(osExchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	err = gameOSQueue.Bind(gameOSExchange.Name, "os")
	//

	gameYearAndAvgPtfExchange, err := setUpYearAndAvgPtfMiddleware(manager)
	if err != nil {
		log.Errorf("Failed to bind accumulator queue: %v", err)
		return
	}

	forever := make(chan bool)

	go mapLines(queue, gameOSExchange, gameYearAndAvgPtfExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapLines(queue *mom.Queue, gameOSExchange *mom.Exchange, gameYearAndAvgPtfExchange *mom.Exchange) error {
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
			log.Info("End of file received")

			for i := 0; i < numNextNodes; i++ {
				gameOSExchange.Publish("os", sp.SerializeMsgEndOfFile())
			}

		case sp.MsgBatch:

			lines, err := sp.DeserializeBatch(d.Body)
			if err != nil {
				log.Error("Error deserializing batch")
				return err
			}

			var gameOsSlice []*oa.GameOS
			var gameYearAndAvgPtfSlice []*df.GameYearAndAvgPtf
			for _, line := range lines {
				log.Debugf("Printing lines: %v", lines)
				reader := csv.NewReader(strings.NewReader(line))
				records, err := reader.Read()

				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				log.Debugf("Printing fields: 0 : %v, 1 : %v, 2 : %v, 3 : %v, 4 : %v", records[0], records[1], records[2], records[3], records[4])

				gameOsSlice, err = createAndAppendGameOS(records, gameOsSlice)
				if err != nil {
					log.Error("error creating and appending game os struct")
					return err
				}

				gameYearAndAvgPtfSlice, err = createAndAppendGameYearAndAvgPtf(records, gameYearAndAvgPtfSlice)
				if err != nil {
					log.Error("error creating and appending game year and avg ptf struct")
					return err
				}

			}

			serializedGameOS := sp.SerializeMsgGameOSInformation(gameOsSlice)
			serializedGameYearAndAvgPtf := sp.SerializeMsgGameYearAndAvgPtf(gameYearAndAvgPtfSlice)

			err = gameOSExchange.Publish("os", serializedGameOS)
			if err != nil {
				log.Error("Error publishing game")
				return err
			}

			err = gameYearAndAvgPtfExchange.Publish("year_avg_ptf", serializedGameYearAndAvgPtf)
			if err != nil {
				log.Error("Error publishing game")
				return err
			}

			log.Infof("Received a message (after attempted send): %s\n", string(d.Body))

		}

	}

	return nil
}

func createAndAppendGameOS(records []string, gameOsSlice []*oa.GameOS) ([]*oa.GameOS, error) {
	gameOs, err := oa.NewGameOS(records[2], records[3], records[4])
	if err != nil {
		log.Error("error creating game os struct")
		return gameOsSlice, err
	}

	gameOsSlice = append(gameOsSlice, gameOs)
	return gameOsSlice, nil
}

func createAndAppendGameYearAndAvgPtf(records []string, gameYearAndAvgPtfSlice []*df.GameYearAndAvgPtf) ([]*df.GameYearAndAvgPtf, error) {
	gameYearAndAvgPtf, err := df.NewGameYearAndAvgPtf(records[0], records[6], records[7])
	if err != nil {
		log.Error("error creating game year and avg ptf struct")
		return gameYearAndAvgPtfSlice, err
	}

	gameYearAndAvgPtfSlice = append(gameYearAndAvgPtfSlice, gameYearAndAvgPtf)
	return gameYearAndAvgPtfSlice, nil
}

// func setUpOsMiddleware(manager *mom.MiddlewareManager) (*mom.Exchange, error) {

// 	gameOSQueue, err := manager.CreateQueue(osQueueName)
// 	if err != nil {
// 		log.Errorf("Failed to declare queue: %v", err)
// 		return nil, err
// 	}

// 	gameOSExchange, err := manager.CreateExchange(osExchangeName, "direct")
// 	if err != nil {
// 		log.Errorf("Failed to declare exchange: %v", err)
// 		return nil, err
// 	}

// 	err = gameOSQueue.Bind(gameOSExchange.Name, osQueueName)

// 	return gameOSExchange, err

// }

func setUpYearAndAvgPtfMiddleware(manager *mom.MiddlewareManager) (*mom.Exchange, error) {

	gameYearAndAvgPtfQueue, err := manager.CreateQueue(yearAndAvgPtfQueueName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return nil, err
	}

	gameYearAndAvgPtfExchange, err := manager.CreateExchange(yearAndAvgPtfExchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return nil, err
	}

	err = gameYearAndAvgPtfQueue.Bind(gameYearAndAvgPtfExchange.Name, "year_avg_ptf")

	return gameYearAndAvgPtfExchange, err

}
