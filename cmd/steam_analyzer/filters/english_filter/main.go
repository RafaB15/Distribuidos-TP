package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	"encoding/csv"
	"io"

	"strings"

	"github.com/op/go-logging"
)

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "reviews_queue"
	queueToSendName    = "english_reviews_queue"
	exchangeName       = "english_reviews_exchange"
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

	englishReviewsExchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	err = queueToSend.Bind(exchangeName, "english_exchange")

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
			englishReviewsExchange.Publish("english_exchange", sp.SerializeMsgEndOfFile())
			log.Info("End of file received")
			break loop
		case sp.MsgBatch:

			lines, err := sp.DeserializeBatch(d.Body)
			if err != nil {
				log.Error("Error deserializing batch")
				return err
			}

			var reviews []*r.Review
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

					reviews = append(reviews, review)

				}

				log.Debugf("Received review: %v", records[1])
			}
			serializedReviews := sp.SerializeMsgReviewInformation(reviews)
			err = englishReviewsExchange.Publish("english_exchange", serializedReviews)
			if err != nil {
				log.Error("Error publishing game")
				return err
			}
		}
	}
	return nil
}
