package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawReviewsExchangeName  = "raw_reviews_exchange"
	RawReviewsExchangeType  = "direct"
	RawReviewsRoutingKey    = "raw_reviews_key"
	RawReviewsEofRoutingKey = "raw_reviews_eof_key"
	RawReviewsQueueName     = "raw_reviews_queue"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"
)

type Middleware struct {
	Manager         *mom.MiddlewareManager
	RawReviewsQueue *mom.Queue
	ReviewsExchange *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	routingKeys := []string{RawReviewsRoutingKey, RawReviewsEofRoutingKey}
	rawReviewsQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(RawReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, routingKeys, false)
	if err != nil {
		return nil, err
	}

	reviewsExchange, err := manager.CreateExchange(ReviewsExchangeName, ReviewsExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:         manager,
		RawReviewsQueue: rawReviewsQueue,
		ReviewsExchange: reviewsExchange,
	}, nil

}

func (m *Middleware) ReceiveGameReviews() ([]string, bool, error) {
	rawMsg, err := m.RawReviewsQueue.Consume()
	if err != nil {
		return nil, false, fmt.Errorf("Failed to consume message: %v", err)
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return nil, false, fmt.Errorf("Failed to deserialize message: %v", err)
	}
	fmt.Printf("Received message from client %d\n", message.ClientID)

	var lines []string

	switch message.MessageType {
	case sp.MsgEndOfFile:
		fmt.Println("Received end of file")
		return nil, true, nil
	case sp.MsgBatch:
		fmt.Println("Received batch")
		lines, err = sp.DeserializeMsgBatch(message.Body)
		if err != nil {
			return nil, false, err
		}
	default:
		return nil, false, fmt.Errorf("unexpected message type: %v", message.MessageType)
	}

	return lines, false, nil
}

func (m *Middleware) SendReviews(reviewsMap map[int][]*r.Review) error {

	for shardingKey, reviews := range reviewsMap {
		routingKey := fmt.Sprintf("%s%d", ReviewsRoutingKeyPrefix, shardingKey)

		serializedReviews := sp.SerializeMsgReviewInformation(reviews)
		err := m.ReviewsExchange.Publish(routingKey, serializedReviews)
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	err := m.RawReviewsQueue.AckLastMessage()
	if err != nil {
		return fmt.Errorf("Failed to ack last message: %v", err)
	}

	return nil
}

func (m *Middleware) SendEndOfFiles(accumulatorsAmount int) error {
	for i := 1; i <= accumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ReviewsRoutingKeyPrefix, i)
		err := m.ReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	err := m.RawReviewsQueue.AckLastMessage()
	if err != nil {
		return fmt.Errorf("Failed to ack last message: %v", err)
	}

	return nil
}
