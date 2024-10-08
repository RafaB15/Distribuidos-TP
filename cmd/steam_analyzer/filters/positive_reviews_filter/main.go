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
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "accumulated_english_reviews_queue"
	//queueToReceiveName2 = "english_reviews_queue_2"
	exchangeName       = "positive_reviews_exchange"
	queueToSendName    = "positive_reviews_queue"
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

	queueToReceive, err := manager.CreateQueue(queueToReceiveName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	queueToSend, err := manager.CreateQueue(queueToSendName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	ReviewsExchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	err = queueToSend.Bind(ReviewsExchange.Name, "positive_review_exchange")
	// vamos a estar mandando a varias colas por sharding

	forever := make(chan bool)

	go filterPositiveReviews(queueToReceive, ReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func filterPositiveReviews(reviewsQueue *mom.Queue, ReviewsExchange *mom.Exchange) error {
	msgs, err := reviewsQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}
	log.Infof("Consuming messages from %s", queueToReceiveName)
loop:
	for d := range msgs {
		msgType, err := sp.DeserializeMessageType(d.Body)
		if err != nil {
			return err
		}

		switch msgType {
		case sp.MsgEndOfFile:
			ReviewsExchange.Publish("positive_review_exchange", sp.SerializeMsgEndOfFile())
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
				routingKey := fmt.Sprintf("positive_review_exchange_%d", shardingKey)
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
