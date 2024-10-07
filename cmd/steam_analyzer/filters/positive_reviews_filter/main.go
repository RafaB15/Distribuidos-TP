package main

import (
	"distribuidos-tp/internal/mom"

	"github.com/op/go-logging"
)

const (
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName = "accumulated_english_reviews_queue"
	//queueToReceiveName2 = "english_reviews_queue_2"
	exchangeName    = "positive_reviews_exchange"
	queueToSendName = "positive_reviews_queue"
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

	err = queueToSend.Bind(ReviewsExchange.Name, "positive_review_exchange")

	forever := make(chan bool)

	go filterPositiveReviews(queueToReceive, ReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func filterPositiveReviews(reviewsQueue *mom.Queue, ReviewsExchange *mom.Exchange) error {
	return nil
}
