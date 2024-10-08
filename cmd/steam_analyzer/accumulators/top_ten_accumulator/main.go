package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	TopTenAccumulatorExchangeName = "top_ten_accumulator_exchange"
	TopTenAccumulatorExchangeType = "direct"
	TopTenAccumulatorRoutingKey   = "top_ten_accumulator_key"
	TopTenAccumulatorQueueName    = "top_ten_accumulator_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"
	numPreviousNodes   = 1
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	// queue, err := manager.CreateQueue(TopTenAccumulatorQueueName)
	// if err != nil {
	// 	log.Errorf("Failed to declare queue: %v", err)
	// 	return
	// }

	topTenAccumulatorQueue, err := manager.CreateBoundQueue(TopTenAccumulatorQueueName, TopTenAccumulatorExchangeName, TopTenAccumulatorExchangeType, TopTenAccumulatorRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	_, err = manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	msgs, err := topTenAccumulatorQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
		return
	}

	forever := make(chan bool)

	go func() error {
		nodesLeft := numPreviousNodes
	loop:
		for d := range msgs {
			messageBody := d.Body
			messageType, err := sp.DeserializeMessageType(messageBody)
			if err != nil {
				return err
			}

			switch messageType {
			case sp.MsgFilteredYearAndAvgPtfInformation:
				log.Infof("Filtered games arrived")
				decadeGames, err := sp.DeserializeMsgGameYearAndAvgPtf(messageBody)
				if err != nil {
					return err
				}
				topTenGames := df.TopTenAvgPlaytimeForever(decadeGames)

				actualTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile("top_ten_games")
				if err != nil {
					log.Errorf("Error uploading top ten games from file: %v", err)
					return err
				}

				updatedTopTenGames := df.TopTenAvgPlaytimeForever(append(topTenGames, actualTopTenGames...))

				err = df.SaveTopTenAvgPlaytimeForeverToFile(updatedTopTenGames, "top_ten_games")
				if err != nil {
					log.Errorf("Error saving top ten games to file: %v", err)
					return err
				}

				nodesLeft -= 1

			default:
				log.Errorf("Unexpected message type: %d", messageType)
				break loop

			}

			if nodesLeft <= 0 {

				finalTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile("top_ten_games")
				if err != nil {
					log.Errorf("Error uploading top ten games from file: %v", err)
					return err
				}
				for _, game := range finalTopTenGames {
					log.Infof("To send Game: %v, Year: %v, AvgPtf: %v", game.AppId, game.ReleaseYear, game.AvgPlaytimeForever)
				}

				break
			}
		}

		return nil
	}()
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
