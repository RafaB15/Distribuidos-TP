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

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"
	ReviewQueueNamePrefix   = "reviews_queue_"

	AccumulatedReviewsExchangeName = "accumulated_reviews_exchange"
	AccumulatedReviewsExchangeType = "direct"
	AccumulatedReviewsRoutingKey   = "accumulated_reviews_key"

	IndieReviewJoinExchangeName             = "indie_review_join_exchange"
	IndieReviewJoinExchangeType             = "direct"
	IndieReviewJoinExchangeRoutingKeyPrefix = "accumulated_reviews_key_"

	IdEnvironmentVariableName            = "ID"
	MappersAmountEnvironmentVariableName = "MAPPERS_AMOUNT"
	IndieReviewJoinersAmountName         = "INDIE_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {

	id, err := u.GetEnv(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	filtersAmount, err := u.GetEnvInt(MappersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	indieReviewJoinerAmount, err := u.GetEnvInt(IndieReviewJoinersAmountName)
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

	reviewQueueName := fmt.Sprintf("%s%s", ReviewQueueNamePrefix, id)
	reviewsRoutingKey := fmt.Sprintf("%s%s", ReviewsRoutingKeyPrefix, id)
	reviewsQueue, err := manager.CreateBoundQueue(reviewQueueName, ReviewsExchangeName, ReviewsExchangeType, reviewsRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	accumulatedReviewsExchange, err := manager.CreateExchange(AccumulatedReviewsExchangeName, AccumulatedReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	indieReviewJoinExchange, err := manager.CreateExchange(IndieReviewJoinExchangeName, IndieReviewJoinExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	forever := make(chan bool)

	go accumulateReviewsMetrics(reviewsQueue, accumulatedReviewsExchange, indieReviewJoinExchange, filtersAmount, indieReviewJoinerAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func accumulateReviewsMetrics(reviewsQueue *mom.Queue, accumulatedReviewsExchange *mom.Exchange, indieReviewJoinExchange *mom.Exchange, filtersAmount int, indieReviewJoinerAmount int) error {
	remainingEOFs := filtersAmount

	accumulatedReviews := make(map[uint32]*ra.GameReviewsMetrics)
	log.Info("Creating Accumulating reviews metrics")
	msgs, err := reviewsQueue.Consume(true)
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

			keyMap := idMapToKeyMap(accumulatedReviews, indieReviewJoinerAmount)
			for routingKey, metrics := range keyMap {
				serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(metrics)

				err = accumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, serializedMetricsBatch)
				if err != nil {
					log.Errorf("Failed to publish metrics: %v", err)
					return err
				}
				log.Info("Published accumulated reviews for percentile")

				err = indieReviewJoinExchange.Publish(routingKey, serializedMetricsBatch)
				if err != nil {
					log.Errorf("Failed to publish metrics: %v", err)
					return err
				}
				log.Infof("Published accumulated reviews for routing key: %d", routingKey)
			}

			err = accumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, sp.SerializeMsgEndOfFile())
			if err != nil {
				log.Errorf("Failed to publish end of file in review accumulator: %v", err)
			}
			log.Info("Published end of file in review accumulator")

			for nodeId := 1; nodeId <= indieReviewJoinerAmount; nodeId++ {
				err = indieReviewJoinExchange.Publish(fmt.Sprintf("%s%d", IndieReviewJoinExchangeRoutingKeyPrefix, nodeId), sp.SerializeMsgEndOfFile())
				if err != nil {
					log.Errorf("Failed to publish end of file in review accumulator: %v", err)
				}
				log.Infof("Published end of file in review accumulator for node %d", nodeId)
			}

			// End of file para las neg reviews accumulator
			// accumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, sp.SerializeMsgEndOfFile())

			break loop

		case sp.MsgReviewInformation:
			log.Infof("Received review information")
			reviews, err := sp.DeserializeMsgReviewInformation(messageBody)
			if err != nil {
				return err
			}
			for _, review := range reviews {
				if metrics, exists := accumulatedReviews[review.AppId]; exists {
					log.Info("Updating metrics for appID: ", review.AppId)
					// Update existing metrics
					metrics.UpdateWithReview(review)
				} else {
					// Create new metrics
					log.Info("Creating new metrics for appID: ", review.AppId)
					newMetrics := ra.NewReviewsMetrics(review.AppId)
					newMetrics.UpdateWithReview(review)
					accumulatedReviews[review.AppId] = newMetrics
				}
			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}
	}
	return nil
}

func idMapToKeyMap(idMap map[uint32]*ra.GameReviewsMetrics, indieReviewJoinerAmount int) map[string][]*ra.GameReviewsMetrics {
	keyMap := make(map[string][]*ra.GameReviewsMetrics)
	for id, metrics := range idMap {
		key := u.GetPartitioningKeyFromInt(int(id), indieReviewJoinerAmount, IndieReviewJoinExchangeRoutingKeyPrefix)
		keyMap[key] = append(keyMap[key], metrics)
	}
	return keyMap
}
