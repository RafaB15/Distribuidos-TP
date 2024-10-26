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
	RawReviewsExchangeType     = "direct"
	RawReviewsRoutingKeyPrefix = "raw_reviews_key_"
	RawReviewsQueueNamePrefix  = "raw_reviews_queue_"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"
)

type Middleware struct {
	Manager         *mom.MiddlewareManager
	RawReviewsQueue *mom.Queue
	ReviewsExchange *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	rawReviewsQueueName := fmt.Sprintf("%s%d", RawReviewsQueueNamePrefix, id)
	rawReviewsRoutingKey := fmt.Sprintf("%s%d", RawReviewsRoutingKeyPrefix, id)

	rawReviewsQueue, err := manager.CreateBoundQueue(rawReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, rawReviewsRoutingKey, false)
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

func (m *Middleware) ReceiveGameReviews() (int, []string, bool, error) {
	rawMsg, err := m.RawReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, fmt.Errorf("failed to consume message: %v", err)
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, fmt.Errorf("failed to deserialize message: %v", err)
	}
	fmt.Printf("Received message from client %d\n", message.ClientID)

	var lines []string

	switch message.Type {
	case sp.MsgEndOfFile:
		fmt.Println("Received end of file")
		return message.ClientID, nil, true, nil
	case sp.MsgBatch:
		fmt.Println("Received batch")
		lines, err = sp.DeserializeMsgBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}
	default:
		return message.ClientID, nil, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}

	return message.ClientID, lines, false, nil
}

func (m *Middleware) SendReviews(clientID int, reviewsMap map[int][]*r.Review) error {

	for shardingKey, reviews := range reviewsMap {
		routingKey := fmt.Sprintf("%s%d", ReviewsRoutingKeyPrefix, shardingKey)

		serializedReviews := sp.SerializeMsgReviewInformation(clientID, reviews)
		err := m.ReviewsExchange.Publish(routingKey, serializedReviews)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
	}

	err := m.RawReviewsQueue.AckLastMessage()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}

	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int, accumulatorsAmount int) error {
	for i := 1; i <= accumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ReviewsRoutingKeyPrefix, i)
		err := m.ReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
	}

	err := m.RawReviewsQueue.AckLastMessage()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}

	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
