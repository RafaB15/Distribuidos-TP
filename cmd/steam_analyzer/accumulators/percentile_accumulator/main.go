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

	numNextNodes = 1
)

var numPrevNodes = 1

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

	go accumulateReviewsMetrics(reviewsQueue, accumulatedReviewsExchange, indieReviewJoinExchange, filtersAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func accumulateReviewsMetrics(reviewsQueue *mom.Queue, accumulatedReviewsExchange *mom.Exchange, indieReviewJoinExchange *mom.Exchange, filtersAmount int) error {

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

			break loop

		case sp.MsgReviewInformation:
			log.Infof("Received review information")
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(d.Body)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}
			err = ra.AddSortedGamesAndMaintainOrder(StoredReviewsFileName, gameReviewsMetrics)
			if err != nil {
				log.Errorf("Failed to add sorted games and maintain order: %v", err)
				return err
			}

			numPrevNodes--
			if numPrevNodes == 0 {
				break loop
			}

		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}
	}

	// Enviar bajo las keys

	return nil
}
