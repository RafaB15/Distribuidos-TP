package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "os_game_queue"
	exchangeName       = "os_accumulator_exchange"
	queueToSendName    = "os_accumulator_queue"
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	queueToReceive, err := manager.CreateQueue(queueToReceiveName)
	if err != nil {
		log.Errorf("Failed to declare game mapper: %v", err)
		return
	}

	exchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	queueToSend, err := manager.CreateQueue(queueToSendName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return
	}

	err = queueToSend.Bind(exchange.Name, "final_accumulator")
	if err != nil {
		log.Errorf("Failed to bind accumulator queue: %v", err)
		return
	}

	msgs, err := queueToReceive.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
		return
	}
	forever := make(chan bool)

	go func() error {
		osMetrics := oa.NewGameOSMetrics()
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
				data, err := oa.LoadGameOsMetricsFromFile("os_metrics")
				if err != nil {
					log.Errorf("Failed to load game os metrics from file: %v", err)
					return err
				}
				msg, err := sp.SerializeMsgAccumulatedGameOSInfo(data)

				if err != nil {
					log.Errorf("Failed to serialize message: %v", err)
					return err
				}

				err = exchange.Publish("final_accumulator", msg)

				if err != nil {
					log.Errorf("Failed to publish message: %v", err)
					return err
				}
				break loop

			case sp.MsgGameOSInformation:
				gamesOs, err := sp.DeserializeMsgGameOSInformation(messageBody)

				if err != nil {
					return err
				}

				for _, gameOs := range gamesOs {
					osMetrics.AddGameOS(gameOs)
				}

				log.Infof("Received Game Os Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)

				if err != nil {
					log.Errorf("Failed to serialize message: %v", err)
				}

				osMetrics.UpdateAndSaveGameOSMetricsToFile("os_metrics")

			default:
				return fmt.Errorf("unexpected message type")

			}
		}

		return nil
	}()

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
