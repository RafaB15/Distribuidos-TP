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
	AccumulatedEnglishReviewsRoutingKey   = "accumulated_english_reviews"
)

type Middleware struct {
	Manager                           *mom.MiddlewareManager
	EnglishReviewsQueue               *mom.Queue
	AccumulatedEnglishReviewsExchange *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware manager: %v", err)
	}

	englishReviewQueueName := fmt.Sprintf("%s%d", EnglishReviewQueueNamePrefix, id)
	englishReviewsRoutingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, id)
	englishReviewsQueue, err := manager.CreateBoundQueue(englishReviewQueueName, EnglishReviewsExchangeName, EnglishReviewsExchangeType, englishReviewsRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	accumulatedEnglishReviewsExchange, err := manager.CreateExchange(AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                           manager,
		EnglishReviewsQueue:               englishReviewsQueue,
		AccumulatedEnglishReviewsExchange: accumulatedEnglishReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveReview() (clientID int, reducedReview *r.ReducedReview, eof bool, e error) {
	rawMsg, err := m.EnglishReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	fmt.Printf("Received message from client %d\n", message.ClientID)

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	case sp.MsgReducedReviewInformation:
		review, err := sp.DeserializeMsgReducedReviewInformation(message.Body)
		if err != nil {
			return message.ClientID, nil, false, fmt.Errorf("failed to deserialize review: %v", err)
		}
		return message.ClientID, review, false, nil
	default:
		return message.ClientID, nil, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendAccumulatedReviews(clientID int, metrics []*ra.NamedGameReviewsMetrics) error {
	serializedMetricsBatch := sp.SerializeMsgNamedGameReviewsMetricsBatch(clientID, metrics)
	err := m.AccumulatedEnglishReviewsExchange.Publish(AccumulatedEnglishReviewsRoutingKey, serializedMetricsBatch)
	if err != nil {
		return fmt.Errorf("failed to publish accumulated reviews: %v", err)
	}
	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int) error {
	serializedEOF := sp.SerializeMsgEndOfFile(clientID)
	err := m.AccumulatedEnglishReviewsExchange.Publish(AccumulatedEnglishReviewsRoutingKey, serializedEOF)
	if err != nil {
		return fmt.Errorf("failed to publish end of file: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
