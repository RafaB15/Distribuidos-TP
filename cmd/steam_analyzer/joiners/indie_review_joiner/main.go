package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
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

	Id = 1
)

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager for indie-review joiner: %v", err)
		return
	}

	defer manager.CloseConnection()

	accumulatedReviewsRoutingKey := fmt.Sprintf("%s%d", AccumulatedReviewsRoutingKeyPrefix, Id)
	indieGameRoutingKey := fmt.Sprintf("%s%d", IndieGameRoutingKeyPrefix, Id)
	indieReviewJoinQueueName := fmt.Sprintf("%s%d", IndieReviewJoinQueueNamePrefix, Id)

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

	go joinActionReviews(indieReviewJoinQueue, topPositiveReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func joinActionReviews(indieReviewJoinQueue *mom.Queue, topPositiveReviewsExchange *mom.Exchange) error {
	accumulatedGameReviews := make(map[uint32]*j.JoinedActionGameReview)
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
			log.Info("End of file received in indie-review joiner")

			topPositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, sp.SerializeMsgEndOfFile())
			break loop

		case sp.MsgGameReviewsMetrics:
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetrics(messageBody)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}
			if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
				log.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
				joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

				serializedMetrics, err := sp.SerializeMsgJoinedActionGameReviews(joinedGameReviewsMsg)
				if err != nil {
					log.Errorf("Failed to serialize indie-review message: %v", err)
				}
				topPositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, serializedMetrics)
				// delete the accumulated review
				delete(accumulatedGameReviews, gameReviewsMetrics.AppID)
			} else {
				log.Info("Saving review for later join")
				accumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedActionGameReview(gameReviewsMetrics.AppID)
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
					serializedMetrics, err := sp.SerializeMsgJoinedActionGameReviews(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to serialize indie-review message: %v", err)
					}
					topPositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, serializedMetrics)
					// delete the accumulated review
					delete(accumulatedGameReviews, indieGame.AppId)
				} else {
					log.Info("Saving indie game for later join")
					newJoinedActionGameReview := j.NewJoinedActionGameReview(indieGame.AppId)
					newJoinedActionGameReview.UpdateWithGame(indieGame)
					accumulatedGameReviews[indieGame.AppId] = newJoinedActionGameReview
				}
			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}

	}

	return nil

}
