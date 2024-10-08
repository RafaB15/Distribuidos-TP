package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"

	"github.com/op/go-logging"
)

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "decade_queue"
	exchangeName       = "writer_exchange"
	queueToSendName    = "writer_queue"
	routingKey         = "top_ten_accumulator"
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

	queue, err := manager.CreateQueue(queueToReceiveName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return
	}

	msgs, err := queue.Consume(true)
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
				decadeGames, err := sp.DeserializeMsgGameYearAndAvgPtf(messageBody)
				if err != nil {
					return err
				}
				topTenGames := df.TopTenAvgPlaytimeForever(decadeGames)

				actualTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile("top_ten_games")
				if err != nil {
					return err
				}

				finalTopTenGames := df.TopTenAvgPlaytimeForever(append(topTenGames, actualTopTenGames...))

				log.Infof("Top ten games: %v", finalTopTenGames)

				nodesLeft -= 1

			default:
				log.Errorf("Unexpected message type: %d", messageType)
				break loop

			}

			if nodesLeft <= 0 {

				break
			}
		}

		return nil
	}()
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
