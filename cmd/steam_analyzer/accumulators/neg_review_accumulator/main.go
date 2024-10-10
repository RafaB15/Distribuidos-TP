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

	AccumulatedReviewsExchangeName = "accumulated_reviews_exchange"
	AccumulatedReviewsExchangeType = "direct"
	AccumulatedReviewsRoutingKey   = "accumulated_reviews_key"
	AccumulatedReviewsQueueName    = "accumulated_reviews_queue"

	CalculatePercentileExchangeName = "calculate_percentile_exchange"
	CalculatePercentileExchangeType = "direct"
	CalculatePercentileRoutingKey   = "calculate_percentile_key"
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	accumulatedReviewsQueue, err := manager.CreateBoundQueue(AccumulatedReviewsQueueName, AccumulatedReviewsExchangeName, AccumulatedReviewsExchangeType, AccumulatedReviewsRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	calculatePercentileExchange, err := manager.CreateExchange(CalculatePercentileExchangeName, CalculatePercentileExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	forever := make(chan bool)

	go sortAndSendAccumulatedReviews(accumulatedReviewsQueue, calculatePercentileExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func sortAndSendAccumulatedReviews(reviewsQueue *mom.Queue, calculatePercentileExchange *mom.Exchange) error {
	msgs, err := reviewsQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}
loop:
	for d := range msgs {

		msgType, err := sp.DeserializeMessageType(d.Body)

		if err != nil {
			log.Errorf("Failed to deserialize message type: %v", err)
			return err
		}

		switch msgType {
		case sp.MsgEndOfFile:

			log.Info("End of file received. Sending end of file message.")
			// Send all acumulated and sorted neg reviews
			gameReviewMetricsToSend, err := ra.ReadGameReviewsMetricsFromFile(StoredReviewsFileName)
			if err != nil {
				log.Errorf("Failed to read game reviews metrics from file: %v", err)
				return err
			}
			log.Infof("Ordered metrics:")
			log.Infof("Number of game review metrics to send: %d", len(gameReviewMetricsToSend))
			for _, review := range gameReviewMetricsToSend {
				log.Infof("Received game review: %v", review.NegativeReviews)
			}

			calculatePercentileExchange.Publish(CalculatePercentileRoutingKey, sp.SerializeMsgGameReviewsMetricsBatch(gameReviewMetricsToSend))
			// calculatePercentileExchange.Publish(CalculatePercentileRoutingKey, sp.SerializeMsgEndOfFile())
			break loop

		case sp.MsgGameReviewsMetrics:
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(d.Body)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}

			err = ra.AddGamesAndMaintainOrder(StoredReviewsFileName, gameReviewsMetrics)
			if err != nil {
				log.Errorf("Failed to add games and maintain order: %v", err)
				return err
			}

		default:
			log.Infof("Received message type: %d", msgType)
		}
	}

	return nil
}
