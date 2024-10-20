package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	EnglishReviewsExchangeName     = "english_reviews_exchange"
	EnglishReviewsExchangeType     = "direct"
	EnglishReviewsRoutingKeyPrefix = "english_reviews_key_"
	EnglishReviewQueueNamePrefix   = "english_reviews_queue_"

	AccumulatedEnglishReviewsExchangeName = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType = "direct"
	AccumulatedEnglishReviewsRoutingKey   = "accumulated_english_reviews_key"
)

type Middleware struct {
	Manager                           *mom.MiddlewareManager
	EnglishReviewsQueue               *mom.Queue
	AccumulatedEnglishReviewsExchange *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, fmt.Errorf("Failed to create middleware manager: %v", err)
	}

	englishReviewQueueName := fmt.Sprintf("%s%d", EnglishReviewQueueNamePrefix, id)
	englishReviewsRoutingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, id)
	englishReviewsQueue, err := manager.CreateBoundQueue(englishReviewQueueName, EnglishReviewsExchangeName, EnglishReviewsExchangeType, englishReviewsRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	accumulatedEnglishReviewsExchange, err := manager.CreateExchange(AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                           manager,
		EnglishReviewsQueue:               englishReviewsQueue,
		AccumulatedEnglishReviewsExchange: accumulatedEnglishReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveReviews() ([]*r.Review, bool, error) {
	rawMsg, err := m.EnglishReviewsQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return nil, false, fmt.Errorf("Failed to deserialize message: %v", err)
	}

	fmt.Printf("Received message from client %d\n", message.ClientID)

	switch message.Type {
	case sp.MsgEndOfFile:
		return nil, true, nil
	case sp.MsgReviewInformation:
		reviews, err := sp.DeserializeMsgReviewInformationV2(message.Body)
		if err != nil {
			return nil, false, fmt.Errorf("Failed to deserialize reviews: %v", err)
		}
		return reviews, false, nil
	default:
		return nil, false, fmt.Errorf("Unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendAccumulatedReviews(metrics []*ra.GameReviewsMetrics) error {
	serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(metrics)
	err := m.AccumulatedEnglishReviewsExchange.Publish(AccumulatedEnglishReviewsRoutingKey, serializedMetricsBatch)
	if err != nil {
		return fmt.Errorf("Failed to publish accumulated reviews: %v", err)
	}
	return nil
}

func (m *Middleware) SendEndOfFiles(positiveReviewsFilterAmount int) error {
	for i := 1; i <= positiveReviewsFilterAmount; i++ {
		serializedEOF := sp.SerializeMsgEndOfFile()
		err := m.AccumulatedEnglishReviewsExchange.Publish(AccumulatedEnglishReviewsRoutingKey, serializedEOF)
		if err != nil {
			return fmt.Errorf("Failed to publish end of file: %v", err)
		}
	}
	return nil
}