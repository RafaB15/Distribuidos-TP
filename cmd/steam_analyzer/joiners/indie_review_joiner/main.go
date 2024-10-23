package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
	u "distribuidos-tp/internal/utils"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	IndieReviewJoinExchangeName        = "indie_review_join_exchange"
	IndieReviewJoinExchangeType        = "direct"
	AccumulatedReviewsRoutingKeyPrefix = "accumulated_reviews_key_"
	IndieGameRoutingKeyPrefix          = "indie_key_"
	IndieReviewJoinQueueNamePrefix     = "indie_review_join_queue_"

	TopPositiveReviewsExchangeName = "top_positive_reviews_exchange"
	TopPositiveReviewsExchangeType = "direct"
	TopPositiveReviewsRoutingKey   = "top_positive_reviews_key"

	ReviewsAccumulatorAmountEnvironmentVariableName = "REVIEWS_ACCUMULATOR_AMOUNT"
	IdEnvironmentVariableName                       = "ID"
)

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager for indie-review joiner: %v", err)
		return
	}

	defer manager.CloseConnection()

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	reviewsAccumulatorAmount, err := u.GetEnvInt(ReviewsAccumulatorAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatedReviewsRoutingKey := fmt.Sprintf("%s%d", AccumulatedReviewsRoutingKeyPrefix, id)
	indieGameRoutingKey := fmt.Sprintf("%s%d", IndieGameRoutingKeyPrefix, id)
	indieReviewJoinQueueName := fmt.Sprintf("%s%d", IndieReviewJoinQueueNamePrefix, id)

	routingKeys := []string{accumulatedReviewsRoutingKey, indieGameRoutingKey}
	indieReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(indieReviewJoinQueueName, IndieReviewJoinExchangeName, IndieReviewJoinExchangeType, routingKeys)
	if err != nil {
		log.Errorf("Failed to create queue for indie-review joiner: %v", err)
		return
	}

	topPositiveReviewsExchange, err := manager.CreateExchange(TopPositiveReviewsExchangeName, TopPositiveReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to create exchange for indie-review joiner: %v", err)
		return
	}

	forever := make(chan bool)

	go joinActionReviews(indieReviewJoinQueue, topPositiveReviewsExchange, reviewsAccumulatorAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func joinActionReviews(indieReviewJoinQueue *mom.Queue, topPositiveReviewsExchange *mom.Exchange, reviewsAccumulatorAmount int) error {
	remainingEOFs := reviewsAccumulatorAmount + 1
	accumulatedGameReviews := make(map[uint32]*j.JoinedPositiveGameReview)
	log.Info("Creating Accumulating reviews metrics")
	msgs, err := indieReviewJoinQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
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
			remainingEOFs--
			log.Info("End of file received in indie-review joiner")
			if remainingEOFs > 0 {
				continue
			}

			topPositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, sp.SerializeMsgEndOfFile())
			break loop

		case sp.MsgGameReviewsMetrics:
			gamesReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(messageBody)
			for _, gameReviewsMetrics := range gamesReviewsMetrics {
				if err != nil {
					log.Errorf("Failed to deserialize game reviews metrics: %v", err)
					return err
				}
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
					log.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

					serializedMetrics, err := sp.SerializeMsgJoinedPositiveGameReviews(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to serialize indie-review message: %v", err)
					}
					topPositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, serializedMetrics)
					// delete the accumulated review
					delete(accumulatedGameReviews, gameReviewsMetrics.AppID)
				} else {
					log.Infof("Saving review with AppID %v for later join", gameReviewsMetrics.AppID)
					accumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedPositiveGameReview(gameReviewsMetrics.AppID)
				}
			}
		case sp.MsgGameNames:

			indieGames, err := sp.DeserializeMsgGameNames(messageBody)
			for _, indieGame := range indieGames {

				if err != nil {
					log.Errorf("Failed to deserialize indie game: %v", err)
					return err
				}
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[indieGame.AppId]; exists {
					log.Infof("Joining indie game into review with ID: %v", indieGame.AppId)
					joinedGameReviewsMsg.UpdateWithGame(indieGame)
					serializedMetrics, err := sp.SerializeMsgJoinedPositiveGameReviews(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to serialize indie-review message: %v", err)
					}
					topPositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, serializedMetrics)
					// delete the accumulated review
					delete(accumulatedGameReviews, indieGame.AppId)
				} else {
					log.Info("Saving indie game with AppID %v for later join", indieGame.AppId)
					newJoinedPositiveGameReview := j.NewJoinedPositiveGameReview(indieGame.AppId)
					newJoinedPositiveGameReview.UpdateWithGame(indieGame)
					accumulatedGameReviews[indieGame.AppId] = newJoinedPositiveGameReview
				}
			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}

	}

	return nil

}
