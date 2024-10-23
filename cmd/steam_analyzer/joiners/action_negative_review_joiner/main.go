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

	// lo que recibo del percentil 90
	ActionPercentileReviewJoinExchangeName     = "action_review_join_exchange"
	ActionPercentileReviewJoinExchangeType     = "direct"
	ActionPercentileReviewJoinRoutingKeyPrefix = "percentile_reviews_key_"

	// lo que recibo del game mapper
	ActionGameRoutingKeyPrefix = "action_key_"

	ActionNegativeReviewJoinQueueNamePrefix = "action_negative_review_join_queue_"

	// lo que voy a estar mandando al writer (esto deberia estar ok porque es igual a todos los otros)
	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"

	IdEnvironmentVariableName = "ID"
)

func main() {

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

	actionReviewJoinRoutingKey := fmt.Sprintf("%s%s", ActionPercentileReviewJoinRoutingKeyPrefix, id)
	actionGameRoutingKey := fmt.Sprintf("%s%s", ActionGameRoutingKeyPrefix, id)

	routingKeys := []string{actionReviewJoinRoutingKey, actionGameRoutingKey}
	actionReviewJoinQueueName := fmt.Sprintf("%s%s", ActionNegativeReviewJoinQueueNamePrefix, id)
	actionReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(actionReviewJoinQueueName, ActionPercentileReviewJoinExchangeName, ActionPercentileReviewJoinExchangeType, routingKeys)
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
	remainingEOFs := 1 + 1 // 1 para el percentil y 1 para los juegos

	accumulatedGameReviews := make(map[uint32]*j.JoinedNegativeGameReview)
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

					serializedMetrics, err := sp.SerializeMsgNegativeJoinedPositiveGameReviews(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to serialize action-review message: %v", err)
					}
					log.Infof("Sending review for game with ID: %v, negative reviews: %d", gameReviewMetrics.AppID, gameReviewMetrics.NegativeReviews)
					writerExchange.Publish(WriterRoutingKey, serializedMetrics)
					// delete the accumulated review
					delete(accumulatedGameReviews, gameReviewMetrics.AppID)
				} else {
					log.Infof("Saving review for later join with id %v", gameReviewMetrics.AppID)
					accumulatedGameReviews[gameReviewMetrics.AppID] = j.NewJoinedActionNegativeGameReview(gameReviewMetrics.AppID)
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
					serializedMetrics, err := sp.SerializeMsgNegativeJoinedPositiveGameReviews(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to serialize action-review message: %v", err)
					}
					log.Infof("Sending review for game with ID: %v, negative reviews: %d", actionGame.AppId)
					writerExchange.Publish(WriterRoutingKey, serializedMetrics)
					// delete the accumulated review
					delete(accumulatedGameReviews, actionGame.AppId)
				} else {
					log.Infof("Saving action game for later join with id %v", actionGame.AppId)
					newJoinedPositiveGameReview := j.NewJoinedActionNegativeGameReview(actionGame.AppId)
					newJoinedPositiveGameReview.UpdateWithGame(actionGame)
					accumulatedGameReviews[actionGame.AppId] = newJoinedPositiveGameReview
				}
			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}

	}

	return nil

}
