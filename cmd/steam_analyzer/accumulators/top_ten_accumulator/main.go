package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	u "distribuidos-tp/internal/utils"
	"os"
	"os/signal"
	"syscall"

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

	DecadeFiltersAmountEnvironmentVariableName = "DECADE_FILTERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	log.Info("Hola que tal")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	decadeFiltersAmount, err := u.GetEnvInt(DecadeFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

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

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}

	forever := make(chan bool)

	go func() error {
		nodesLeft := decadeFiltersAmount
	loop:
		for d := range msgs {
			messageBody := d.Body
			messageType, err := sp.DeserializeMessageType(messageBody)
			if err != nil {
				return err
			}
			log.Infof("Message type: %v", messageType)
			switch messageType {
			case sp.MsgEndOfFile:
				nodesLeft -= 1
			case sp.MsgFilteredYearAndAvgPtfInformation:
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

			default:
				log.Errorf("Unexpected message type: %d", messageType)
				break loop

			}
			// log.Infof("Nodes left: %v", nodesLeft)

			if nodesLeft <= 0 {

				finalTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile("top_ten_games")
				if err != nil {
					log.Errorf("Error uploading top ten games from file: %v", err)
					return err
				}

				for _, game := range finalTopTenGames {
					log.Infof("To send Game: %v, Year: %v, AvgPtf: %v", game.AppId, game.ReleaseYear, game.AvgPlaytimeForever)
				}
				log.Infof("Final top ten games: %v", finalTopTenGames)
				srzGames := df.SerializeTopTenAvgPlaytimeForever(finalTopTenGames)
				bytes := sp.SerializeTopTenDecadeAvgPtfQueryMsg(srzGames)

				err = writerExchange.Publish(WriterRoutingKey, bytes)

				if err != nil {
					log.Errorf("Failed to publish message: %v", err)
					return err
				}
				log.Debug("Top ten games sent to writer")

				err = writerExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
				if err != nil {
					log.Errorf("Failed to publish end of file: %v", err)
					return err
				}
				log.Debug("End of file sent to writer")

				break
			}
		}

		return nil
	}()
	log.Info("Waiting for messages. To exit press CTRL+C")
	go func() {
		sig := <-sigs
		log.Infof("Received signal: %v. Waiting for tasks to complete...", sig)
		log.Info("All tasks completed. Shutting down.")
		done <- true
	}()

	<-done
	<-forever
}
