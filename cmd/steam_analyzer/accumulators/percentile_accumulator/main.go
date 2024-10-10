package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	u "distribuidos-tp/internal/utils"
	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	StoredReviewsFileName = "stored_reviews"

	CalculatePercentileQueueName    = "calculate_percentile_queue"
	CalculatePercentileExchangeName = "calculate_percentile_exchange"
	CalculatePercentileExchangeType = "direct"
	CalculatePercentileRoutingKey   = "calculate_percentile_key"

	AccumulatedPercentileReviewsExchangeName     = "action_review_join_exchange"
	AccumulatedPercentileReviewsExchangeType     = "direct"
	AccumulatedPercentileReviewsRoutingKeyPrefix = "percentile_reviews_key_"

	ActionNegativeReviewsJoinersAmountEnvironmentVariableName = "ACTION_NEGATIVE_REVIEWS_JOINERS_AMOUNT"

	IdEnvironmentVariableName = "ID"
	NumPreviousAccumulators   = "NUM_PREVIOUS_ACCUMULATORS"
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

	actionNegativeReviewsJoinersAmount, err := u.GetEnvInt(ActionNegativeReviewsJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	previousAccumulators, err := u.GetEnvInt(NumPreviousAccumulators)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	percentileQueue, err := manager.CreateBoundQueue(CalculatePercentileQueueName, CalculatePercentileExchangeName, CalculatePercentileExchangeType, CalculatePercentileRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	percentileExchange, err := manager.CreateExchange(AccumulatedPercentileReviewsExchangeName, AccumulatedPercentileReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	forever := make(chan bool)

	go calculatePercentile(percentileQueue, percentileExchange, previousAccumulators, actionNegativeReviewsJoinersAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func calculatePercentile(percentileQueue *mom.Queue, percentileExchange *mom.Exchange, previousAccumulators int, actionNegativeReviewsJoinersAmount int) error {
	remainingEOFs := previousAccumulators
	accumulatedPercentileKeyMap := make(map[string][]*ra.GameReviewsMetrics)
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
			log.Info("End Of File for accumulated reviews received")
			remainingEOFs--
			if remainingEOFs > 0 {
				continue
			}

			//obtengo los reviews de percentil en una lista
			abovePercentile, err := ra.GetTop10PercentByNegativeReviews(StoredReviewsFileName)
			if err != nil {
				log.Errorf("Failed to get top 10 percent by negative reviews: %v", err)
				return err
			}

			// por cada review le calculo la sharding key y lo inserto
			for id, review := range abovePercentile {
				key := u.GetPartitioningKeyFromInt(int(id), actionNegativeReviewsJoinersAmount, AccumulatedPercentileReviewsRoutingKeyPrefix)
				accumulatedPercentileKeyMap[key] = append(accumulatedPercentileKeyMap[key], review)
				log.Infof("Metrics above p90: id:%v #:%v", review.AppID, review.NegativeReviews)
			}

			// por cada key serializo y envio
			for routingKey, metrics := range accumulatedPercentileKeyMap {
				serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(metrics)

				err = percentileExchange.Publish(routingKey, serializedMetricsBatch)
				if err != nil {
					log.Errorf("Failed to publish metrics: %v", err)
					return err
				}
				log.Info("Published accumulated reviews for join")
				log.Infof("Published accumulated reviews for routing key: %d", routingKey)
			}

			// envio a todos los joiners end of file
			err = handleEof(percentileExchange, actionNegativeReviewsJoinersAmount)
			if err != nil {
				log.Errorf("Failed to handle end of file: %v", err)
			}
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

		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}
	}

	return nil
}

func handleEof(percentileExchange *mom.Exchange, actionNegativeReviewsJoinersAmount int) error {
	for i := 1; i <= actionNegativeReviewsJoinersAmount; i++ {
		routingKey := fmt.Sprintf("%v%d", AccumulatedPercentileReviewsRoutingKeyPrefix, i)
		err := percentileExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
		if err != nil {
			return err
		}
	}
	return nil
}
