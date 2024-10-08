package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	YearAndAvgPtfExchangeName = "year_and_avg_ptf_exchange"
	YearAndAvgPtfExchangeType = "direct"
	YearAndAvgPtfRoutingKey   = "year_and_avg_ptf_key"
	YearAndAvgPtfQueueName    = "year_and_avg_ptf_queue"

	decade = 2010
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	yearAndAvgPtfQueue, err := manager.CreateBoundQueue(YearAndAvgPtfQueueName, YearAndAvgPtfExchangeName, YearAndAvgPtfExchangeType, YearAndAvgPtfRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	exchange, err := manager.CreateExchange(topTenAccumulatorExchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	queueToSend, err := manager.CreateQueue(decadeQueue)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return
	}

	err = queueToSend.Bind(exchange.Name, routingKey)
	if err != nil {
		log.Errorf("Failed to bind accumulator queue: %v", err)
		return
	}

	msgs, err := yearAndAvgPtfQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
		return
	}
	forever := make(chan bool)

	go func() error {
	loop:
		for d := range msgs {
			messageBody := d.Body
			messageType, err := sp.DeserializeMessageType(messageBody)

			if err != nil {
				return err
			}

			switch messageType {

			case sp.MsgEndOfFile:
				log.Info("End of file arrived")
				break loop

			case sp.MsgGameYearAndAvgPtfInformation:

				gamesYearsAvgPtfs, err := sp.DeserializeMsgGameYearAndAvgPtf(messageBody)

				if err != nil {
					return err
				}

				gamesYearsAvgPtfsFiltered := df.FilterByDecade(gamesYearsAvgPtfs, decade)

				for _, game := range gamesYearsAvgPtfsFiltered {
					log.Infof("Game: %v, Year: %v, AvgPtf: %v", game.AppId, game.ReleaseYear, game.AvgPlaytimeForever)
				}

				msg := sp.SerializeMsgGameYearAndAvgPtf(gamesYearsAvgPtfsFiltered)

				err = exchange.Publish(routingKey, msg)
				if err != nil {
					return err
				}

			default:
				return fmt.Errorf("unexpected message type")

			}
		}

		return nil
	}()

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
