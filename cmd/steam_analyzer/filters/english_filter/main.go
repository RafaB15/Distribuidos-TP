package main

import (
	"crypto/sha256"
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

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

	rawEnglishReviewsQueue, err := manager.CreateBoundQueue(RawEnglishReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, "")
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	englishReviewsExchange, err := manager.CreateExchange(EnglishReviewsExchangeName, EnglishReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	forever := make(chan bool)

	go filterEnglishReviews(rawEnglishReviewsQueue, englishReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func filterEnglishReviews(rawEnglishReviewsQueue *mom.Queue, englishReviewsExchange *mom.Exchange) error {
	languageIdentifier := r.NewLanguageIdentifier()

	msgs, err := rawEnglishReviewsQueue.Consume(true)
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
			englishReviewsExchange.Publish("english_reviews_key_1", sp.SerializeMsgEndOfFile())
			//englishReviewsExchange.Publish("english_reviews_exchange_2", sp.SerializeMsgEndOfFile())
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
				log.Debugf("Printing fields: 0 : %v, 2 : %v", records[0], records[2])

				review, err := r.NewReviewFromStrings(records[0], records[2])
				if err != nil {
					log.Error("Problema creando review con texto")
					return err
				}

				if languageIdentifier.IsEnglish(records[1]) {
					log.Debugf("I am the english language")
					appID, err := strconv.Atoi(records[0]) //el appID esta en el records 0
					if err != nil {
						log.Errorf("Failed to convert AppID: %v", err)
						return err
					}

					//reviews = append(reviews, review)
					reviewSlice := []*r.Review{review}
					serializedReview := sp.SerializeMsgReviewInformation(reviewSlice)
					routingKey := getRoutingKey(appID, numNextNodes)
					log.Debugf("Routing key: %s", routingKey)
					err = englishReviewsExchange.Publish(routingKey, serializedReview)
					if err != nil {
						log.Errorf("Failed to publish message: %v", err)
						return err
					}

				}

				log.Debugf("Received review: %v", records[1])
			}

		}
	}
	return nil
}

func calculateShardingKey(appID int, numShards int) int {
	// hash function xa dist mejor
	appIDStr := fmt.Sprintf("%d", appID)
	hash := sha256.Sum256([]byte(appIDStr))
	hashInt := binary.BigEndian.Uint64(hash[:8])
	return int(hashInt%uint64(numShards)) + 1 // oo=jo con el +1. Hay que cambiarlo cuando escalemos el sistema. Modulo de algo con 1 siempre es 0.
}

func getRoutingKey(appId int, numPartitions int) string {
	shardingKey := calculateShardingKey(appId, numPartitions) // Note: Adjust the +1 if needed for your sharding logic
	routingKey := fmt.Sprintf("%v%d", EnglishReviewsRoutingKeyPrefix, shardingKey)
	return routingKey
}
