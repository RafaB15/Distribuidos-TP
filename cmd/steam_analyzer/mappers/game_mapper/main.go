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
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"
	RawGamesQueueName    = "raw_games_queue"

	OSGamesExchangeName = "os_games_exchange"
	OSGamesRoutingKey   = "os_games_key"
	OSGamesExchangeType = "direct"

	YearAndAvgPtfExchangeName = "year_and_avg_ptf_exchange"
	YearAndAvgPtfExchangeType = "direct"
	YearAndAvgPtfRoutingKey   = "year_and_avg_ptf_key"

	numNextNodes = 2
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	rawGamesQueue, err := manager.CreateBoundQueue(RawGamesQueueName, RawGamesExchangeName, RawGamesExchangeType, RawGamesRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	osGamesExchange, err := manager.CreateExchange(OSGamesExchangeName, OSGamesExchangeType)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}

	gameYearAndAvgPtfExchange, err := manager.CreateExchange(YearAndAvgPtfExchangeName, YearAndAvgPtfExchangeType)
	if err != nil {
		log.Errorf("Failed to bind accumulator queue: %v", err)
		return
	}

	forever := make(chan bool)

	go mapLines(rawGamesQueue, osGamesExchange, gameYearAndAvgPtfExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapLines(rawGamesQueue *mom.Queue, osGamesExchange *mom.Exchange, gameYearAndAvgPtfExchange *mom.Exchange) error {
	msgs, err := rawGamesQueue.Consume(true)
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
				osGamesExchange.Publish(OSGamesRoutingKey, sp.SerializeMsgEndOfFile())
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

			err = osGamesExchange.Publish(OSGamesRoutingKey, serializedGameOS)
			if err != nil {
				log.Error("Error publishing game")
				return err
			}

			err = gameYearAndAvgPtfExchange.Publish(YearAndAvgPtfRoutingKey, serializedGameYearAndAvgPtf)
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
