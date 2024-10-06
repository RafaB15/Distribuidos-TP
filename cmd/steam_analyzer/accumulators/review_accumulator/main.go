package main

import (
	"distribuidos-tp/internal/mom"

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
	// deberia saber si ya estoy acumulando las reviews del id recibido. si no, entonces creo un nuevo reviewMetrics

loop:
	for d := range msgs {
		messageBody := d.Body
		messageType, err := sp.DeserializeMessageType(messageBody)
		if err != nil {
			return err
		}

		switch messageType {
		case sp.MessageEndOfFile:
			log.Info("End Of File for Accumulate Reviews rceived")
		case sp.MsgReviewInformation:
			// revisar si existe ya el id o deberiamos crear uno nuevo

		}
	}
	return nil
}
