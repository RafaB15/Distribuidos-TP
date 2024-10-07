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
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "reviews_queue"
	//queueToSendName        = "english_reviews_queue_1"
	queueToSendNameReview1 = "english_reviews_queue_1"
	//queueToSendNameReview2 = "english_reviews_queue_2"
	exchangeName = "english_reviews_exchange"
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

	queueToReceive, err := manager.CreateQueue(queueToReceiveName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	englishReviewsExchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	queueToSend1, err := manager.CreateQueue(queueToSendNameReview1)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}
	err = queueToSend1.Bind(englishReviewsExchange.Name, "english_reviews_exchange_1")
	//err = queueToSend2.Bind(englishReviewsExchange.Name, "english_reviews_exchange_2")
	//bindear todos los demas

	forever := make(chan bool)

	go filterEnglishReviews(queueToReceive, englishReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func filterEnglishReviews(reviewsQueue *mom.Queue, englishReviewsExchange *mom.Exchange) error {
	languageIdentifier := r.NewLanguageIdentifier()

	msgs, err := reviewsQueue.Consume(true)
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
			englishReviewsExchange.Publish("english_reviews_exchange_1", sp.SerializeMsgEndOfFile())
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
				log.Debugf("Printing lines: %v", lines)
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

				if languageIdentifier.IsEnglish(records[1]) {
					log.Debugf("I am the english language")
					appID, err := strconv.Atoi(records[0]) //el appID esta en el records 0
					if err != nil {
						log.Errorf("Failed to convert AppID: %v", err)
						return err
					}

					shardingKey := calculateShardingKey(appID, numNextNodes)
					log.Infof("Sharding key: %d", shardingKey)

					//reviews = append(reviews, review)
					reviewSlice := []*r.Review{review}
					serializedReview := sp.SerializeMsgReviewInformation(reviewSlice)
					routingKey := fmt.Sprintf("english_reviews_exchange_%d", shardingKey)
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
