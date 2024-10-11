package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
	OSAccumulatorQueueName    = "os_accumulator_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"

	numPreviousNodes = 2
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

	osAccumulatorQueue, err := manager.CreateBoundQueue(OSAccumulatorQueueName, OSAccumulatorExchangeName, OSAccumulatorExchangeType, OSAccumulatorRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}

	msgs, err := osAccumulatorQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
		return
	}

	forever := make(chan bool)

	go func() error {
		nodesLeft := numPreviousNodes
		finalGameMetrics := oa.NewGameOSMetrics()
	loop:
		for d := range msgs {
			messageBody := d.Body

			messageType, err := sp.DeserializeMessageType(messageBody)
			if err != nil {
				return err
			}

			switch messageType {
			case sp.MsgAccumulatedGameOSInformation:
				gameMetrics, err := sp.DeserializeMsgAccumulatedGameOSInformation(messageBody)
				if err != nil {
					return err
				}
				finalGameMetrics.Merge(gameMetrics)
				nodesLeft -= 1
				log.Infof("Successfully merged new game OS metrics information accumulated deserialized. Nodes left: %v", nodesLeft)
				log.Infof("Windows Metrics: %v", finalGameMetrics.Windows)
				log.Infof("Linux Metrics: %v", finalGameMetrics.Linux)
				log.Infof("Mac Metrics: %v", finalGameMetrics.Mac)
			default:
				log.Errorf("Unexpected message type: %d", messageType)
				break loop

			}

			if nodesLeft <= 0 {
				sendToWriter(writerExchange, finalGameMetrics)
				break
			}
		}

		return nil
	}()
	log.Info("Waiting for messages. To exit press CTRL+C")

	go func() {
		sig := <-sigs
		log.Infof("Received signal: %v. Waiting for tasks to complete...", sig)
		wg.Wait() // Esperar a que todas las tareas en el WaitGroup terminen
		log.Info("All tasks completed. Shutting down.")
		done <- true

	}()

	<-done

	<-forever
}

func sendToWriter(writerExchange *mom.Exchange, finalGameMetrics *oa.GameOSMetrics) error {
	srz_metrics := oa.SerializeGameOSMetrics(finalGameMetrics)

	msg := sp.SerializeOsResolvedQueryMsg(srz_metrics)

	err := writerExchange.Publish(WriterRoutingKey, msg)
	if err != nil {
		log.Errorf("Failed to publish message: %v", err)
		return err
	}

	err = writerExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		log.Errorf("Failed to publish end of file: %v", err)
		return err
	}

	return nil
}
