package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawReviewsExchangeName = "raw_reviews_exchange"
	RawReviewsExchangeType = "fanout"
	RawReviewsQueueName    = "raw_reviews_queue"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"

	numNextNodes = 1
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	rawReviewsQueue, err := manager.CreateBoundQueue(RawReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, "")
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	reviewsExchange, err := manager.CreateExchange(ReviewsExchangeName, ReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	forever := make(chan bool)

	go mapReviews(rawReviewsQueue, reviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapReviews(rawReviewsQueue *mom.Queue, reviewsExchange *mom.Exchange) error {
	msgs, err := rawReviewsQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}

loop:
	for d := range msgs {

		msgType, err := sp.DeserializeMessageType(d.Body)
		if err != nil {
			return err
		}

		switch msgType {
		case sp.MsgEndOfFile:
			for i := 1; i < numNextNodes+1; i++ {
				reviewsExchange.Publish(fmt.Sprintf("%v%d", ReviewsRoutingKeyPrefix, i), sp.SerializeMsgEndOfFile())
			}
			log.Info("End of file received")
			break loop
		case sp.MsgBatch:

			lines, err := sp.DeserializeBatch(d.Body)
			if err != nil {
				log.Error("Error deserializing batch")
				return err
			}

			for _, line := range lines {
				reader := csv.NewReader(strings.NewReader(line))
				records, err := reader.Read()

				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				log.Debugf("Printing fields: 0 : %v, 1 : %v, 2 : %v", records[0], records[1], records[2])

				review, err := r.NewReviewFromStrings(records[0], records[2])
				if err != nil {
					log.Error("Problema creando review con texto")
					return err
				}

				shardingKey := u.CalculateShardingKey(records[0], numNextNodes)
				log.Infof("Sharding key: %d", shardingKey)

				reviewSlice := []*r.Review{review}
				serializedReview := sp.SerializeMsgReviewInformation(reviewSlice)
				routingKey := fmt.Sprintf("%v%d", ReviewsRoutingKeyPrefix, shardingKey)
				err = reviewsExchange.Publish(routingKey, serializedReview)

				log.Debugf("Received review: %v", records[1])
			}

		}
	}

	return nil
}
