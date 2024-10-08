package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "action_review_queue"
	//queueToReceiveName2 = "english_reviews_queue_2"
	exchangeName    = "action_review_exchange"
	queueToSendName = "writer_queue"
)

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager for action-review joiner: %v", err)
		return
	}

	defer manager.CloseConnection()

	queueToReceive, err := manager.CreateQueue(queueToReceiveName)
	if err != nil {
		log.Errorf("Failed to declare queue for action-review joiner: %v", err)
	}

	queueToSend, err := manager.CreateQueue(queueToSendName)
	if err != nil {
		log.Errorf("Failed to declare queue for action-review joiner: %v", err)
	}

	ReviewsExchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange for action-review joiner: %v", err)

	}

	err = queueToSend.Bind(ReviewsExchange.Name, "action_review_exchange")

	forever := make(chan bool)

	go joinActionReviews(queueToReceive, ReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func joinActionReviews(reviewsQueue *mom.Queue, ReviewsExchange *mom.Exchange) error {
	accumulatedGameReviews := make(map[uint32]*j.JoinedActionGameReview)
	log.Info("Creating Accumulating reviews metrics")
	msgs, err := reviewsQueue.Consume(true)
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

			ReviewsExchange.Publish("action_review_exchange", sp.SerializeMsgEndOfFile())
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
				ReviewsExchange.Publish("action_review_exchange", serializedMetrics)
				// delete the accumulated review
				delete(accumulatedGameReviews, gameReviewsMetrics.AppID)
			} else {
				log.Info("Saving review for later join")
				accumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedActionGameReview(gameReviewsMetrics.AppID)
			}
		case sp.MsgActionGame:
			actionGame, err := sp.DeserializeMsgActionGame(messageBody)
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
				ReviewsExchange.Publish("action_review_exchange", serializedMetrics)
				// delete the accumulated review
				delete(accumulatedGameReviews, actionGame.AppId)
			} else {
				log.Info("Saving action game for later join")
				newJoinedActionGameReview := j.NewJoinedActionGameReview(actionGame.AppId)
				newJoinedActionGameReview.UpdateWithGame(actionGame)
				accumulatedGameReviews[actionGame.AppId] = newJoinedActionGameReview

			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}

	}

	return nil

}
