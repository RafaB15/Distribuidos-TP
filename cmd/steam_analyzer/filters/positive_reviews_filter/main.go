package main

import (
	"crypto/sha256"
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	"encoding/binary"
	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	Id = 1

	AccumulatedEnglishReviewsExchangeName = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType = "direct"
	//AccumulatedReviewsRoutingKeyPrefix = "accumulated_reviews_key"
	AccumulatedEnglishReviewsRoutingKey = "accumulated_english_reviews_key"
	AccumulatedEnglishReviewQueueName   = "accumulated_english_reviews_queue"

	PositiveJoinReviewsExchangeName     = "action_review_join_exchange"
	PositiveJoinReviewsExchangeType     = "direct"
	PositiveJoinReviewsRoutingKeyPrefix = "positive_reviews_key_"

	numPreviousNodes   = 1
	numNextNodes       = 1
	minPositiveReviews = 5
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	//AccumulatedReviewQueueName := fmt.Sprintf("%s%d", AccumulatedReviewQueueNamePrefix, Id)
	//accumulatedReviewsRoutingKey := fmt.Sprintf("%s%d", AccumulatedReviewsRoutingKeyPrefix, Id)
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

	go filterPositiveReviews(accumulatedEnglishReviewsQueue, positiveJoinedEnglishReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func filterPositiveReviews(reviewsQueue *mom.Queue, ReviewsExchange *mom.Exchange) error {
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
			ReviewsExchange.Publish("positive_reviews_key_1", sp.SerializeMsgEndOfFile())
			break loop
		case sp.MsgGameReviewsMetrics:
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetrics(d.Body)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}

			log.Infof("Received game reviews metrics: %v", gameReviewsMetrics)
			log.Infof("Positive reviews of appID: %v, %v", gameReviewsMetrics.AppID, gameReviewsMetrics.PositiveReviews)

			// esta en 5 porque como estamos con un dataset reducido no hay juegos con tantas reviews positivas
			if gameReviewsMetrics.PositiveReviews > minPositiveReviews {
				log.Infof("Review metric: appID: %v, with positive reviews: %v", gameReviewsMetrics.AppID, gameReviewsMetrics.PositiveReviews)
				shardingKey := calculateShardingKey(int(gameReviewsMetrics.AppID), numNextNodes)
				routingKey := fmt.Sprintf("positive_reviews_key_%d", shardingKey)
				serializedMetric, err := sp.SerializeMsgGameReviewsMetrics(gameReviewsMetrics)
				err = ReviewsExchange.Publish(routingKey, serializedMetric)
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

func calculateShardingKey(appID int, numShards int) int {
	// hash function xa dist mejor
	appIDStr := fmt.Sprintf("%d", appID)
	hash := sha256.Sum256([]byte(appIDStr))
	hashInt := binary.BigEndian.Uint64(hash[:8])
	return int(hashInt%uint64(numShards)) + 1 // oo=jo con el +1. Hay que cambiarlo cuando escalemos el sistema. Modulo de algo con 1 siempre es 0.
}
