package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
	u "distribuidos-tp/internal/utils"
	"sort"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	TopPositiveReviewsExchangeName = "top_positive_reviews_exchange"
	TopPositiveReviewsEchangeType  = "direct"
	TopPositiveReviewsRoutingKey   = "top_positive_reviews_key"
	TopPositiveReviewsQueueName    = "top_positive_reviews_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"

	IndieReviewJoinersAmountEnvironmentVariableName = "INDIE_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {

	indieReviewJoinerAmount, err := u.GetEnvInt(IndieReviewJoinersAmountEnvironmentVariableName)
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

	topPositiveReviewsQueue, err := manager.CreateBoundQueue(TopPositiveReviewsQueueName, TopPositiveReviewsExchangeName, TopPositiveReviewsEchangeType, TopPositiveReviewsRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	_, err = manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}

	forever := make(chan bool)

	go FilterTopPositiveReviews(topPositiveReviewsQueue, writerExchange, indieReviewJoinerAmount)

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func FilterTopPositiveReviews(topPositiveReviewsQueue *mom.Queue, writerExchange *mom.Exchange, indieReviewJoinerAmount int) error {
	topPositiveIndieGames := make([]*j.JoinedPositiveGameReview, 0)
	nodesLeft := indieReviewJoinerAmount
	msgs, err := topPositiveReviewsQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
		return err
	}
loop:
	for d := range msgs {
		messageBody := d.Body
		messageType, err := sp.DeserializeMessageType(messageBody)
		if err != nil {
			return err
		}

		switch messageType {
		case sp.MsgEndOfFile:
			log.Infof("Received EOF message in top positive reviews node")
			nodesLeft--
			if nodesLeft > 0 {
				continue
			}
			serializedTop := sp.SerializeMsgJoinedIndieGameReviewsBatch(topPositiveIndieGames)
			err = writerExchange.Publish(WriterRoutingKey, serializedTop)
			if err != nil {
				log.Errorf("Failed to publish message: %v", err)
				return err
			}
			log.Info("Sent IndieGame to writer")
			err := writerExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
			if err != nil {
				log.Errorf("Failed to publish EOF message: %v", err)
				return err
			}
			break loop
		case sp.MsgQueryResolved:
			log.Infof("Received query resolved message in top positive reviews node")
			indieGame, err := sp.DeserializeMsgJoinedPositiveGameReviews(messageBody)
			if err != nil {
				log.Errorf("Failed to deserialize message: %v", err)
				return err
			}

			log.Infof("Received indie game with ID: %v", indieGame.AppId)
			log.Infof("Evauating number of positive reviews and saving game")
			topPositiveIndieGames = append(topPositiveIndieGames, indieGame)
			if len(topPositiveIndieGames) > 5 {
				// Sort the slice by positive reviews in descending order
				sort.Slice(topPositiveIndieGames, func(i, j int) bool {
					return topPositiveIndieGames[i].PositiveReviews > topPositiveIndieGames[j].PositiveReviews
				})
				// Keep only the top 5
				topPositiveIndieGames = topPositiveIndieGames[:5]
			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}
	}
	return nil
}
