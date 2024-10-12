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

	Id = 1

	AccumulatedEnglishReviewsExchangeName = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType = "direct"
	AccumulatedEnglishReviewsRoutingKey   = "accumulated_english_reviews_key"
	AccumulatedEnglishReviewQueueName     = "accumulated_english_reviews_queue"

	PositiveJoinReviewsExchangeName     = "action_review_join_exchange"
	PositiveJoinReviewsExchangeType     = "direct"
	PositiveJoinReviewsRoutingKeyPrefix = "positive_reviews_key_"

	ActionPositiveReviewsJoinersAmountEnvironmentVariableName = "ACTION_POSITIVE_REVIEWS_JOINERS_AMOUNT"
	EnglishReviewAccumulatorsAmountEnvironmentVariableName    = "ENGLISH_REVIEW_ACCUMULATORS_AMOUNT"

	minPositiveReviews = 100
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	actionReviewsJoinersAmount, err := u.GetEnvInt(ActionPositiveReviewsJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	englishReviewAccumulatorsAmount, err := u.GetEnvInt(EnglishReviewAccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatedEnglishReviewsQueue, err := manager.CreateBoundQueue(AccumulatedEnglishReviewQueueName, AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType, AccumulatedEnglishReviewsRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	positiveJoinedEnglishReviewsExchange, err := manager.CreateExchange(PositiveJoinReviewsExchangeName, PositiveJoinReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	forever := make(chan bool)

	go filterPositiveReviews(accumulatedEnglishReviewsQueue, positiveJoinedEnglishReviewsExchange, actionReviewsJoinersAmount, englishReviewAccumulatorsAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func filterPositiveReviews(reviewsQueue *mom.Queue, ReviewsExchange *mom.Exchange, actionReviewsJoinersAmount int, englishReviewAccumulatorsAmount int) error {
	remainingEOFs := englishReviewAccumulatorsAmount
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
			log.Info("End of file received.")
			remainingEOFs--
			if remainingEOFs > 0 {
				continue
			}

			for i := 1; i <= actionReviewsJoinersAmount; i++ {
				routingKey := fmt.Sprintf("%s%d", PositiveJoinReviewsRoutingKeyPrefix, i)
				ReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
			}
			break loop
		case sp.MsgGameReviewsMetrics:
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(d.Body)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}

			positiveReviewsMap := make(map[string][]*ra.GameReviewsMetrics)

			for _, gameReviewMetrics := range gameReviewsMetrics {
				if gameReviewMetrics.PositiveReviews > minPositiveReviews {
					log.Infof("Review metric: appID: %v, with positive reviews: %v", gameReviewMetrics.AppID, gameReviewMetrics.PositiveReviews)
					updatePositiveReviewsMap(gameReviewMetrics, positiveReviewsMap, actionReviewsJoinersAmount)
				}
			}

			for routingKey, gameReviewsMetrics := range positiveReviewsMap {
				serializedMetrics := sp.SerializeMsgGameReviewsMetricsBatch(gameReviewsMetrics)
				err = ReviewsExchange.Publish(routingKey, serializedMetrics)
				if err != nil {
					log.Errorf("Failed to publish game reviews metrics: %v", err)
					return err
				}
			}
		default:
			log.Infof("Received message type: %d", msgType)
		}

	}
	return nil
}

func updatePositiveReviewsMap(gameReviewMetrics *ra.GameReviewsMetrics, routingKeyMap map[string][]*ra.GameReviewsMetrics, accumulatorsAmount int) {
	reoutingKey := u.GetPartitioningKeyFromInt(int(gameReviewMetrics.AppID), accumulatorsAmount, PositiveJoinReviewsRoutingKeyPrefix)
	routingKeyMap[reoutingKey] = append(routingKeyMap[reoutingKey], gameReviewMetrics)
}
