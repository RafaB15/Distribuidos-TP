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

	ActionReviewJoinExchangeName     = "action_review_join_exchange"
	ActionReviewJoinExchangeType     = "direct"
	ActionReviewJoinRoutingKeyPrefix = "positive_reviews_key_"
	ActionGameRoutingKeyPrefix       = "action_key_"
	ActionReviewJoinQueueName        = "action_review_join_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"

	Id = 1
)

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager for action-review joiner: %v", err)
		return
	}

	defer manager.CloseConnection()

	actionReviewJoinRoutingKey := fmt.Sprintf("%s%d", ActionReviewJoinRoutingKeyPrefix, Id)
	actionGameRoutingKey := fmt.Sprintf("%s%d", ActionGameRoutingKeyPrefix, Id)

	routingKeys := []string{actionReviewJoinRoutingKey, actionGameRoutingKey}
	actionReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(ActionReviewJoinQueueName, ActionReviewJoinExchangeName, ActionReviewJoinExchangeType, routingKeys)
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

	go joinActionReviews(actionReviewJoinQueue, writerExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func joinActionReviews(actionReviewJoinQueue *mom.Queue, writerExchange *mom.Exchange) error {
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
			log.Info("End of file received in action-review joiner")

			writerExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
			break loop

		case sp.MsgGameReviewsMetrics:
			gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetrics(messageBody)
			if err != nil {
				log.Errorf("Failed to deserialize game reviews metrics: %v", err)
				return err
			}
			if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
				log.Infof("Joining review into action game with ID: %v", gameReviewsMetrics.AppID)
				joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

				serializedMetrics, err := sp.SerializeMsgJoinedActionGameReviews(joinedGameReviewsMsg)
				if err != nil {
					log.Errorf("Failed to serialize action-review message: %v", err)
				}
				writerExchange.Publish(WriterRoutingKey, serializedMetrics)
				// delete the accumulated review
				delete(accumulatedGameReviews, gameReviewsMetrics.AppID)
			} else {
				log.Info("Saving review for later join")
				accumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedActionGameReview(gameReviewsMetrics.AppID)
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
					log.Info("Saving action game for later join")
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
