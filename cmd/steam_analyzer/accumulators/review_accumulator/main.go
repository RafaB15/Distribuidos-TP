package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"

	"github.com/op/go-logging"
)

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "english_reviews_queue_1"
	exchangeName       = "accumulated_english_reviews_exchange"
	queueToSendName    = "accumulated_english reviews_queue"
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

	ReviewsExchange, err := manager.CreateExchange(exchangeName, "direct")
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}

	err = queueToSend.Bind(exchangeName, "review_accumulator_exchange")

	forever := make(chan bool)

	go accumulateEnglishReviewsMetrics(queueToReceive, ReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func accumulateEnglishReviewsMetrics(reviewsQueue *mom.Queue, ReviewsExchange *mom.Exchange) error {
	accumulatedReviews := make(map[uint32]*ra.GameReviewsMetrics)
	msgs, err := reviewsQueue.Consume(true)
	if err != nil {
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
			log.Info("End Of File for Accumulate Reviews received")
			break loop
		case sp.MsgReviewInformation:
			reviews, err := sp.DeserializeMsgReviewInformation(messageBody)
			if err != nil {
				return err
			}
			for _, review := range reviews {
				if metrics, exists := accumulatedReviews[review.AppId]; exists {
					// Update existing metrics
					metrics.UpdateWithReview(review)
				} else {
					// Create new metrics
					newMetrics := ra.NewReviewsMetrics(review.AppId)
					newMetrics.UpdateWithReview(review)
					accumulatedReviews[review.AppId] = newMetrics
				}
			}
		}
	}
	return nil
}
