package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"

	"github.com/op/go-logging"
)

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "os_accumulator_queue"
	exchangeName       = "writer_exchange"
	queueToSendName    = "writer_queue"
	routingKey         = "os_final_accumulator"
	numPreviousNodes   = 2
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
				sendToWriter(manager, finalGameMetrics)
				break
			}
		}

		return nil
	}()
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func sendToWriter(manager *mom.MiddlewareManager, finalGameMetrics *oa.GameOSMetrics) error {
	queueToSend, err := manager.CreateQueue(queueToSendName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return err
	}

	exchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return err
	}

	err = queueToSend.Bind(exchange.Name, routingKey)
	if err != nil {
		log.Errorf("Failed to bind accumulator queue: %v", err)
		return err
	}

	srz_metrics := oa.SerializeGameOSMetrics(finalGameMetrics)

	msg := sp.SerializeOsResolvedQueryMsg(srz_metrics)

	err = exchange.Publish(routingKey, msg)
	if err != nil {
		log.Errorf("Failed to publish message: %v", err)
		return err
	}

	return nil
}
