package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"

	"github.com/op/go-logging"
)

const (
	middlewareURI       = "amqp://guest:guest@rabbitmq:5672/"
	queueToReceiveName  = "english_reviews_queue_1"
	queueToReceiveName2 = "english_reviews_queue_2"
	exchangeName        = "accumulated_english_reviews_exchange"
	queueToSendName     = "accumulated_english_reviews_queue"
	numNextNodes        = 1
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

	err = queueToSend.Bind(ReviewsExchange.Name, "review_accumulator_exchange")

	forever := make(chan bool)

	go accumulateEnglishReviewsMetrics(queueToReceive, ReviewsExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func accumulateEnglishReviewsMetrics(reviewsQueue *mom.Queue, ReviewsExchange *mom.Exchange) error {
	accumulatedReviews := make(map[uint32]*ra.GameReviewsMetrics)
	log.Info("Creating Accumulating reviews metrics")
	msgs, err := reviewsQueue.Consume(true)
	if err != nil {
		return err
	}
loop:
	for d := range msgs {
		messageBody := d.Body
		messageType, err := sp.DeserializeMessageType(messageBody)
		if err != nil {
			log.Errorf("Failed to deserialize message type: %v", err)
			return err
		}
		log.Infof("Received message type: %d", messageType)

		switch messageType {
		case sp.MsgEndOfFile:
			log.Info("End Of File for accumulated reviews received")

			// solo hacer esto si recibi todos los mensajes de end of file
			for _, metrics := range accumulatedReviews {
				log.Info("Serializing accumulated reviews")

				serializedMetrics, err := r.SerializeGameReviewsMetrics(metrics)
				if err != nil {
					log.Errorf("Failed to serialize accumulated reviews: %v", err)
					return err
				}

				err = ReviewsExchange.Publish("review_accumulator_exchange", serializedMetrics)
				if err != nil {
					log.Errorf("Failed to publish metrics: %v", err)
					return err
				}

			}

			//serialize msg de metrics
			// hay que mandarselo a todos los nodos de filtro de 5k. tipo fanout.
			err = ReviewsExchange.Publish("review_accumulator_exchange", sp.SerializeMsgEndOfFile())
			if err != nil {
				log.Errorf("Failed to publish end of file in review accumulator: %v", err)
			}
			break loop

		case sp.MsgReviewInformation:
			reviews, err := sp.DeserializeMsgReviewInformation(messageBody)
			if err != nil {
				return err
			}
			for _, review := range reviews {
				if metrics, exists := accumulatedReviews[review.AppId]; exists {
					log.Info("Updating metrics for appID: ", review.AppId)
					// Update existing metrics
					metrics.UpdateWithReview(review)
				} else {
					// Create new metrics
					log.Info("Creating new metrics for appID: ", review.AppId)
					newMetrics := ra.NewReviewsMetrics(review.AppId)
					newMetrics.UpdateWithReview(review)
					accumulatedReviews[review.AppId] = newMetrics
				}
			}
		default:
			log.Errorf("Unexpected message type: %d", messageType)
		}
	}
	return nil
}
