package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	EnglishReviewsExchangeName     = "english_reviews_exchange"
	EnglishReviewsExchangeType     = "direct"
	EnglishReviewsRoutingKeyPrefix = "english_reviews_key_"
	EnglishReviewQueueNamePrefix   = "english_reviews_queue_"

	AccumulatedEnglishReviewsExchangeName     = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType     = "direct"
	AccumulatedEnglishReviewsRoutingKeyPrefix = "accumulated_english_reviews_key_"
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

func (m *Middleware) ReceiveReview() (int, *r.Review, bool, error) {
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
	case sp.MsgReviewInformation:
		review, err := sp.DeserializeMsgReviewInformation(message.Body)
		if err != nil {
			return message.ClientID, nil, false, fmt.Errorf("failed to deserialize review: %v", err)
		}
		return message.ClientID, review, false, nil
	default:
		return message.ClientID, nil, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendAccumulatedReviews(clientID int, negativeReviewsFiltersAmount int, metrics []*ra.GameReviewsMetrics) error {
	nodeId := u.GetRandomNumber(negativeReviewsFiltersAmount)
	routingKey := fmt.Sprintf("%s%d", AccumulatedEnglishReviewsRoutingKeyPrefix, nodeId)
	serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(clientID, metrics)
	err := m.AccumulatedEnglishReviewsExchange.Publish(routingKey, serializedMetricsBatch)
	if err != nil {
		return fmt.Errorf("failed to publish accumulated reviews: %v", err)
	}
	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int, negativeReviewsFilterAmount int) error {
	for i := 1; i <= negativeReviewsFilterAmount; i++ {
		serializedEOF := sp.SerializeMsgEndOfFile(clientID)
		routingKey := fmt.Sprintf("%s%d", AccumulatedEnglishReviewsRoutingKeyPrefix, i)
		fmt.Printf("Sending EOF to %s\n", routingKey)
		err := m.AccumulatedEnglishReviewsExchange.Publish(routingKey, serializedEOF)
		if err != nil {
			return fmt.Errorf("failed to publish end of file: %v", err)
		}
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
