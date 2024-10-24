package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawReviewsExchangeName           = "raw_reviews_exchange"
	RawReviewsExchangeType           = "direct"
	RawEnglishReviewsKeyPrefix       = "raw_english_reviews_key_"
	RawEnglishReviewsQueueNamePrefix = "raw_english_reviews_queue_"

	EnglishReviewsExchangeName     = "english_reviews_exchange"
	EnglishReviewsExchangeType     = "direct"
	EnglishReviewsRoutingKeyPrefix = "english_reviews_key_"
)

type Middleware struct {
	Manager                *mom.MiddlewareManager
	RawEnglishReviewsQueue *mom.Queue
	EnglishReviewsExchange *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, fmt.Errorf("Failed to create middleware manager: %v", err)
	}

	rawEnglishReviewsQueueName := fmt.Sprintf("%s%d", RawEnglishReviewsQueueNamePrefix, id)
	rawEnglishReviewsRoutingKey := fmt.Sprintf("%s%d", RawEnglishReviewsKeyPrefix, id)

	rawEnglishReviewsQueue, err := manager.CreateBoundQueue(rawEnglishReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, rawEnglishReviewsRoutingKey, false)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	englishReviewsExchange, err := manager.CreateExchange(EnglishReviewsExchangeName, EnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                manager,
		RawEnglishReviewsQueue: rawEnglishReviewsQueue,
		EnglishReviewsExchange: englishReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveGameReviews() (int, []string, bool, error) {
	rawMsg, err := m.RawEnglishReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, fmt.Errorf("Failed to consume message: %v", err)
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, fmt.Errorf("Failed to deserialize message: %v", err)
	}
	fmt.Printf("Received message from client %d\n", message.ClientID)
	fmt.Printf("Received message type %d\n", message.Type)
	fmt.Printf("Received message body %d\n", len(message.Body))

	var lines []string

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	case sp.MsgBatch:
		lines, err = sp.DeserializeMsgBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}
	default:
		return message.ClientID, nil, false, fmt.Errorf("unexpected message type: %d", message.Type)
	}

	return message.ClientID, lines, false, nil
}

func (m *Middleware) SendEnglishReviews(clientID int, reviewsMap map[int][]*r.Review) error {
	for shardingKey, reviews := range reviewsMap {
		routingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, shardingKey)

		serializedReviews := sp.SerializeMsgReviewInformationV2(clientID, reviews)
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

func (m *Middleware) SendEndOfFiles(clientID int, accumulatorsAmount int) error {
	for i := 1; i <= accumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, i)
		err := m.EnglishReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFileV2(clientID))
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
