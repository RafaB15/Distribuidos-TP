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
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "raw_reviews_queue"
	queueToSendName    = "reviews_queue"
	exchangeName       = "reviews_exchange"
	routingKeyPrefix   = "reviews_exchange"
	numNextNodes       = 1
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

	queueToSend, err := manager.CreateQueue(queueToSendName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	exchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	queueToSend.Bind(exchange.Name, queueToSendName)

	forever := make(chan bool)

	go mapReviews(queueToReceive, exchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapReviews(reviewsQueue *mom.Queue, exchange *mom.Exchange) error {
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
			exchange.Publish(queueToSendName, sp.SerializeMsgEndOfFile())
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

				shardingKey := u.CalculateShardingKey(records[0], numNextNodes)
				log.Infof("Sharding key: %d", shardingKey)

				reviewSlice := []*r.Review{review}
				serializedReview := sp.SerializeMsgReviewInformation(reviewSlice)
				routingKey := fmt.Sprintf("%d_%d", routingKeyPrefix, shardingKey)
				err = exchange.Publish(routingKey, serializedReview)

				log.Debugf("Received review: %v", records[1])
			}

		}
	}

	return nil
}
