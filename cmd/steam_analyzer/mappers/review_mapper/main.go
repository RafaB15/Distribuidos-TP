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
	"time"

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

	RawReviewsEofExchangeName    = "raw_reviews_eof_exchange"
	RawReviewsEofExchangeType    = "fanout"
	RawReviewsEofQueueNamePrefix = "raw_reviews_eof_queue_"

	AccumulatorsAmountEnvironmentVariableName = "ACCUMULATORS_AMOUNT"
	IdEnvironmentVariableName                 = "ID"
)

var log = logging.MustGetLogger("log")

func main() {
	id, err := u.GetEnv(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatorsAmount, err := u.GetEnvInt(AccumulatorsAmountEnvironmentVariableName)
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

	rawReviewsQueue, err := manager.CreateBoundQueue(RawReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, "")
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	rawReviewsEofQueueName := fmt.Sprintf("%s%s", RawReviewsEofQueueNamePrefix, id)
	rawReviewsEofQueue, err := manager.CreateBoundQueue(rawReviewsEofQueueName, RawReviewsEofExchangeName, RawReviewsEofExchangeType, "")
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

	go mapReviews(rawReviewsQueue, rawReviewsEofQueue, reviewsExchange, accumulatorsAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapReviews(rawReviewsQueue *mom.Queue, rawReviewsEofQueue *mom.Queue, reviewsExchange *mom.Exchange, accumulatorsAmount int) error {
	msgs, err := rawReviewsQueue.Consume(false)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}

	timeout := time.Second * 1

loop:
	for {
		select {
		case d := <-msgs:
			msgType, err := sp.DeserializeMessageType(d.Body)
			if err != nil {
				return err
			}

			if msgType == sp.MsgBatch {
				lines, err := sp.DeserializeBatch(d.Body)
				if err != nil {
					log.Error("Error deserializing batch")
					return err
				}
				err = handleMsgBatch(lines, reviewsExchange, accumulatorsAmount)
				if err != nil {
					log.Errorf("Failed to handle batch: %v", err)
					return err
				}
			}
			d.Ack(false)
		case <-time.After(timeout):
			eofMsg, err := rawReviewsEofQueue.GetIfAvailable()
			if err != nil {
				timeout = time.Second * 1
				continue
			}
			msgType, err := sp.DeserializeMessageType(eofMsg.Body)
			if err != nil {
				log.Error("Error deserializing message type from EOF")
			}
			if msgType == sp.MsgEndOfFile {
				err := handleEof(reviewsExchange, accumulatorsAmount)
				if err != nil {
					log.Errorf("Failed to handle EOF: %v", err)
					return err
				}
				log.Info("End of file received")
				break loop
			}
		}
	}
	return nil
}

func handleMsgBatch(lines []string, reviewsExchange *mom.Exchange, accumulatorsAmount int) error {
	routingKeyMap := make(map[string][]*r.Review)

	for _, line := range lines {
		reader := csv.NewReader(strings.NewReader(line))
		records, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		log.Debugf("Printing fields: 0 : %v, 2 : %v", records[0], records[2])

		review, err := r.NewReviewFromStrings(records[0], records[2])
		if err != nil {
			log.Error("Problema creando review con texto")
			return err
		}
		updateReviewsMap(review, routingKeyMap, accumulatorsAmount)
		log.Debug("Updated reviews map")
	}

	for routingKey, reviews := range routingKeyMap {
		serializedReviews := sp.SerializeMsgReviewInformation(reviews)
		log.Debugf("Routing key: %s", routingKey)
		err := reviewsExchange.Publish(routingKey, serializedReviews)
		if err != nil {
			log.Errorf("Failed to publish message: %v", err)
			return err
		}
	}

	return nil
}

func handleEof(reviewsExchange *mom.Exchange, accumulatorsAmount int) error {
	for i := 1; i <= accumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%v%d", ReviewsRoutingKeyPrefix, i)
		err := reviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
		if err != nil {
			return err
		}
	}
	return nil
}

func updateReviewsMap(review *r.Review, routingKeyMap map[string][]*r.Review, accumulatorsAmount int) {
	reoutingKey := u.GetPartitioningKeyFromInt(int(review.AppId), accumulatorsAmount, ReviewsRoutingKeyPrefix)
	routingKeyMap[reoutingKey] = append(routingKeyMap[reoutingKey], review)
}
