package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	StoredReviewsFileName = "stored_reviews"

	CalculatePercentileQueueName    = "calculate_percentile_queue"
	CalculatePercentileExchangeName = "calculate_percentile_exchange"
	CalculatePercentileExchangeType = "direct"
	CalculatePercentileRoutingKey   = "calculate_percentile_key"

	AccumulatedReviewsExchangeName = "accumulated_reviews_exchange"
	AccumulatedReviewsExchangeType = "direct"
	AccumulatedReviewsRoutingKey   = "accumulated_reviews_key"

	IdEnvironmentVariableName            = "ID"
	MappersAmountEnvironmentVariableName = "MAPPERS_AMOUNT"

	numNextNodes = 1
)

var numPrevNodes = 1

var log = logging.MustGetLogger("log")

func main() {

	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	percentileQueue, err := manager.CreateBoundQueue(CalculatePercentileQueueName, CalculatePercentileExchangeName, CalculatePercentileExchangeType, CalculatePercentileRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	percentileExchange, err := manager.CreateExchange(CalculatePercentileExchangeName, CalculatePercentileExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	forever := make(chan bool)

	go calculatePercentile(percentileQueue, percentileExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func calculatePercentile(percentileQueue *mom.Queue, percentileExchange *mom.Exchange) error {

	msgs, err := percentileQueue.Consume(true)
	if err != nil {
		return err
	}
loop:
	for d := range msgs {
		messageBody := d.Body
		messageType, err := sp.DeserializeMessageType(messageBody)
		if err != nil {
			log.Errorf("Failed to deserialize message type: %v", err)
			return err
		}

		switch messageType {
		case sp.MsgEndOfFile:

			break loop

		case sp.MsgGameReviewsMetrics:
			log.Infof("Received review information")
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(d.Body)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}
			for _, review := range gameReviewsMetrics {
				log.Infof("Received game review metrics: %v", review.NegativeReviews)
			}

			err = ra.AddSortedGamesAndMaintainOrder(StoredReviewsFileName, gameReviewsMetrics)
			if err != nil {
				log.Errorf("Failed to add sorted games and maintain order: %v", err)
				return err
			}

			numPrevNodes--
			if numPrevNodes == 0 {

				abovePercentile, err := ra.GetTop10PercentByNegativeReviews(StoredReviewsFileName)
				if err != nil {
					log.Errorf("Failed to get top 10 percent by negative reviews: %v", err)
					return err
				}

				for _, review := range abovePercentile {
					log.Infof("Metrics above p90: id:%v #:%v", review.AppID, review.NegativeReviews)
				}

				break loop
			}

		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}
	}

	// Enviar bajo las keys

	return nil
}
