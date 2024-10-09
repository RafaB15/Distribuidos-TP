package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"strings"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawReviewsExchangeName     = "raw_reviews_exchange"
	RawReviewsExchangeType     = "fanout"
	RawEnglishReviewsQueueName = "raw_english_reviews_queue"

	EnglishReviewsExchangeName     = "english_reviews_exchange"
	EnglishReviewsExchangeType     = "direct"
	EnglishReviewsRoutingKeyPrefix = "english_reviews_key_"

	RawReviewsEofExchangeName           = "raw_reviews_eof_exchange"
	RawReviewsEofExchangeType           = "fanout"
	RawEnglishReviewsEofQueueNamePrefix = "raw_english_reviews_eof_queue_"

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

	rawEnglishReviewsQueue, err := manager.CreateBoundQueue(RawEnglishReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, "")
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	rawEnglishReviewsEofQueueName := fmt.Sprintf("%s%s", RawEnglishReviewsEofQueueNamePrefix, id)
	rawEnglishReviewsEofQueue, err := manager.CreateBoundQueue(rawEnglishReviewsEofQueueName, RawReviewsEofExchangeName, RawReviewsEofExchangeType, "")
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	englishReviewsExchange, err := manager.CreateExchange(EnglishReviewsExchangeName, EnglishReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	forever := make(chan bool)

	go filterEnglishReviews(rawEnglishReviewsQueue, rawEnglishReviewsEofQueue, englishReviewsExchange, accumulatorsAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func filterEnglishReviews(rawEnglishReviewsQueue *mom.Queue, rawEnglishReviewsEofQueue *mom.Queue, englishReviewsExchange *mom.Exchange, accumulatorsAmount int) error {
	languageIdentifier := r.NewLanguageIdentifier()

	msgs, err := rawEnglishReviewsQueue.Consume(true)
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
				err = handleMsgBatch(lines, englishReviewsExchange, accumulatorsAmount, languageIdentifier)
				if err != nil {
					log.Errorf("Failed to handle batch: %v", err)
					return err
				}
			}
		case <-time.After(timeout):
			eofMsg, err := rawEnglishReviewsEofQueue.GetIfAvailable()
			if err != nil {
				timeout = time.Second * 1
				continue
			}
			msgType, err := sp.DeserializeMessageType(eofMsg.Body)
			if err != nil {
				log.Error("Error deserializing message type from EOF")
			}
			if msgType == sp.MsgEndOfFile {
				err := handleEof(englishReviewsExchange, accumulatorsAmount)
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

func handleMsgBatch(lines []string, englishReviewsExchange *mom.Exchange, accumulatorsAmount int, languageIdentifier *r.LanguageIdentifier) error {
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

		if languageIdentifier.IsEnglish(records[1]) {
			log.Debugf("I am the english language")
			if err != nil {
				log.Errorf("Failed to convert AppID: %v", err)
				return err
			}

			//reviews = append(reviews, review)
			reviewSlice := []*r.Review{review}
			serializedReview := sp.SerializeMsgReviewInformation(reviewSlice)
			routingKey := u.GetPartitioningKey(records[0], accumulatorsAmount, EnglishReviewsRoutingKeyPrefix)
			log.Debugf("Routing key: %s", routingKey)
			err = englishReviewsExchange.Publish(routingKey, serializedReview)
			if err != nil {
				log.Errorf("Failed to publish message: %v", err)
				return err
			}

		}

		log.Debugf("Received review: %v", records[1])
	}
	return nil
}

func handleEof(englishReviewsExchange *mom.Exchange, accumulatorsAmount int) error {
	for i := 1; i <= accumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%v%d", EnglishReviewsRoutingKeyPrefix, i)
		err := englishReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
		if err != nil {
			return err
		}
	}
	return nil
}
