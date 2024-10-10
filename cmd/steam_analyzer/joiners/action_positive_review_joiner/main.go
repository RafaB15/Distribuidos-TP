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

	ActionReviewJoinExchangeName     = "action_review_join_exchange"
	ActionReviewJoinExchangeType     = "direct"
	ActionReviewJoinRoutingKeyPrefix = "positive_reviews_key_"
	ActionGameRoutingKeyPrefix       = "action_key_"
	ActionReviewJoinQueueNamePrefix  = "action_review_join_queue_"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"

	IdEnvironmentVariableName                           = "ID"
	PositiveReviewsFiltersAmountEnvironmentVariableName = "POSITIVE_REVIEWS_FILTERS_AMOUNT"
)

func main() {
	positiveReviewsFiltersAmount, err := u.GetEnvInt(PositiveReviewsFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	id, err := u.GetEnv(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager for action-review joiner: %v", err)
		return
	}

	defer manager.CloseConnection()

	actionReviewJoinRoutingKey := fmt.Sprintf("%s%s", ActionReviewJoinRoutingKeyPrefix, id)
	actionGameRoutingKey := fmt.Sprintf("%s%s", ActionGameRoutingKeyPrefix, id)

	routingKeys := []string{actionReviewJoinRoutingKey, actionGameRoutingKey}
	actionReviewJoinQueueName := fmt.Sprintf("%s%s", ActionReviewJoinQueueNamePrefix, id)
	actionReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(actionReviewJoinQueueName, ActionReviewJoinExchangeName, ActionReviewJoinExchangeType, routingKeys)
	if err != nil {
		log.Errorf("Failed to create queue for action-review joiner: %v", err)
		return
	}

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		log.Errorf("Failed to create exchange for action-review joiner: %v", err)
		return
	}

	forever := make(chan bool)

	go joinActionReviews(actionReviewJoinQueue, writerExchange, positiveReviewsFiltersAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func joinActionReviews(actionReviewJoinQueue *mom.Queue, writerExchange *mom.Exchange, positiveReviewsFiltersAmount int) error {
	remainingEOFs := positiveReviewsFiltersAmount + 1

	accumulatedGameReviews := make(map[uint32]*j.JoinedActionGameReview)
	log.Info("Creating Accumulating reviews metrics")
	msgs, err := actionReviewJoinQueue.Consume(true)
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
			if remainingEOFs > 0 {
				continue
			}

			log.Info("End of file received in action-review joiner")

			writerExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
			break loop

		case sp.MsgGameReviewsMetrics:
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(messageBody)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}

			for _, gameReviewMetrics := range gameReviewsMetrics {
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewMetrics.AppID]; exists {
					log.Infof("Joining review into action game with ID: %v", gameReviewMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewMetrics)

					serializedMetrics, err := sp.SerializeMsgJoinedActionGameReviews(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to serialize action-review message: %v", err)
					}
					writerExchange.Publish(WriterRoutingKey, serializedMetrics)
					// delete the accumulated review
					delete(accumulatedGameReviews, gameReviewMetrics.AppID)
				} else {
					log.Infof("Saving review for later join with id %v", gameReviewMetrics.AppID)
					accumulatedGameReviews[gameReviewMetrics.AppID] = j.NewJoinedActionGameReview(gameReviewMetrics.AppID)
				}
			}
		case sp.MsgGameNames:

			actionGames, err := sp.DeserializeMsgGameNames(messageBody)
			for _, actionGame := range actionGames {

				if err != nil {
					log.Errorf("Failed to deserialize action game: %v", err)
					return err
				}
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[actionGame.AppId]; exists {
					log.Infof("Joining action game into review with ID: %v", actionGame.AppId)
					joinedGameReviewsMsg.UpdateWithGame(actionGame)
					serializedMetrics, err := sp.SerializeMsgJoinedActionGameReviews(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to serialize action-review message: %v", err)
					}
					writerExchange.Publish(WriterRoutingKey, serializedMetrics)
					// delete the accumulated review
					delete(accumulatedGameReviews, actionGame.AppId)
				} else {
					log.Infof("Saving action game for later join with id %v", actionGame.AppId)
					newJoinedActionGameReview := j.NewJoinedActionGameReview(actionGame.AppId)
					newJoinedActionGameReview.UpdateWithGame(actionGame)
					accumulatedGameReviews[actionGame.AppId] = newJoinedActionGameReview
				}
			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}

	}

	return nil

}
