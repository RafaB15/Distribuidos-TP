package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	OSGamesExchangeName = "os_games_exchange"
	OSGamesRoutingKey   = "os_games_key"
	OSGamesExchangeType = "direct"
	OSGamesQueueName    = "os_games_queue"

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
)

var log = logging.MustGetLogger("log")
var wg sync.WaitGroup // WaitGroup para sincronizar la finalización

func main() {

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}

	defer manager.CloseConnection()

	osGamesQueue, err := manager.CreateBoundQueue(OSGamesQueueName, OSGamesExchangeName, OSGamesExchangeType, OSGamesRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	osAccumulatorExchange, err := manager.CreateExchange(OSAccumulatorExchangeName, OSAccumulatorExchangeType)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}

	msgs, err := osGamesQueue.Consume(true)
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

				err = osAccumulatorExchange.Publish(OSAccumulatorRoutingKey, msg)

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

				wg.Add(1)
				osMetrics.UpdateAndSaveGameOSMetricsToFile("os_metrics", &wg)

			default:
				return fmt.Errorf("unexpected message type")

			}
		}

		return nil
	}()

	go func() {
		sig := <-sigs
		log.Infof("Received signal: %v. Waiting for tasks to complete...", sig)
		wg.Wait() // Esperar a que todas las tareas en el WaitGroup terminen
		log.Info("All tasks completed. Shutting down.")
		done <- true
	}()

	<-done

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}
