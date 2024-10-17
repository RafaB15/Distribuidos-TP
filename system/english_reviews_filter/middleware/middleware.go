package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawReviewsExchangeName     = "raw_reviews_exchange"
	RawReviewsExchangeType     = "fanout"
	RawEnglishReviewsQueueName = "raw_english_reviews_queue"

	EnglishReviewsExchangeName     = "english_reviews_exchange"
	EnglishReviewsExchangeType     = "direct"
	EnglishReviewsRoutingKeyPrefix = "english_reviews_key_"

	RawReviewsEofExchangeName           = "raw_reviews_eof_exchange"
	RawReviewsEofExchangeType           = "fanout"
	RawEnglishReviewsEofQueueNamePrefix = "raw_english_reviews_eof_queue_"
)

type Middleware struct {
	RawEnglishReviewsQueue    *mom.Queue
	RawEnglishReviewsEofQueue *mom.Queue
	EnglishReviewsExchange    *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, fmt.Errorf("Failed to create middleware manager: %v", err)
	}
	defer manager.CloseConnection()

	rawEnglishReviewsQueue, err := manager.CreateBoundQueue(RawEnglishReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, "", false)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	rawEnglishReviewsEofQueueName := fmt.Sprintf("%s%d", RawEnglishReviewsEofQueueNamePrefix, id)
	rawEnglishReviewsEofQueue, err := manager.CreateBoundQueue(rawEnglishReviewsEofQueueName, RawReviewsEofExchangeName, RawReviewsEofExchangeType, "", false)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	englishReviewsExchange, err := manager.CreateExchange(EnglishReviewsExchangeName, EnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	return &Middleware{
		RawEnglishReviewsQueue:    rawEnglishReviewsQueue,
		RawEnglishReviewsEofQueue: rawEnglishReviewsEofQueue,
		EnglishReviewsExchange:    englishReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveGameReviews() ([]string, bool, error) {
	msg, err := m.RawEnglishReviewsQueue.Consume()
	if err != nil {
		return nil, false, fmt.Errorf("Failed to consume message: %v", err)
	}

	messageType, err := sp.DeserializeMessageType(msg)
	if err != nil {
		return nil, false, err
	}

	var lines []string

	switch messageType {
	case sp.MsgEndOfFile:
		return nil, true, nil
	case sp.MsgBatch:
		lines, err = sp.DeserializeBatch(msg)
		if err != nil {
			return nil, false, err
		}
	default:
		return nil, false, fmt.Errorf("unexpected message type: %d", messageType)
	}

	return lines, false, nil
}

func (m *Middleware) SendEnglishReviews(reviewsMap map[int][]*r.Review) error {
	for shardingKey, reviews := range reviewsMap {
		routingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, shardingKey)

		serializedReviews := sp.SerializeMsgReviewInformation(reviews)
		err := m.EnglishReviewsExchange.Publish(routingKey, serializedReviews)
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	err := m.RawEnglishReviewsQueue.AckLastMessage()
	if err != nil {
		return fmt.Errorf("Failed to ack last message: %v", err)
	}

	return nil
}

func (m *Middleware) SendEndOfFiles(accumulatorsAmount int) error {
	for i := 1; i <= accumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, i)
		err := m.EnglishReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	return nil
}
