package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"

	RawReviewsExchangeName  = "raw_reviews_exchange"
	RawReviewsExchangeType  = "direct"
	RawReviewsRoutingKey    = "raw_reviews_key"
	RawEnglishReviewsEofKey = "raw_english_reviews_eof_key"
	RawReviewsEofKey        = "raw_reviews_eof_key"

	QueryResponseQueueName    = "query_response_queue"
	QueryResponseExchangeName = "query_response_exchange"
	QueryResponseExchangeType = "direct"
	QueryResponseRoutingKey   = "query_response_key"
)

type Middleware struct {
	Manager            *mom.MiddlewareManager
	RawGamesExchange   *mom.Exchange
	RawReviewsExchange *mom.Exchange
	QueryResponseQueue *mom.Queue
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		return nil, fmt.Errorf("Failed to create middleware manager: %v", err)
	}

	rawGamesExchange, err := manager.CreateExchange(RawGamesExchangeName, RawGamesExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	rawReviewsExchange, err := manager.CreateExchange(RawReviewsExchangeName, RawReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	queryResponseQueue, err := manager.CreateBoundQueue(QueryResponseQueueName, QueryResponseExchangeName, QueryResponseExchangeType, QueryResponseRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	return &Middleware{
		Manager:            manager,
		RawGamesExchange:   rawGamesExchange,
		RawReviewsExchange: rawReviewsExchange,
		QueryResponseQueue: queryResponseQueue,
	}, nil
}

func (m *Middleware) SendGamesBatch(clientID int, data []byte) error {
	batch := sp.SerializeMsgBatch(clientID, data)
	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, batch)
	if err != nil {
		return fmt.Errorf("Failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendReviewsBatch(clientID int, data []byte) error {
	batch := sp.SerializeMsgBatch(clientID, data)
	err := m.RawReviewsExchange.Publish(RawReviewsRoutingKey, batch)
	if err != nil {
		return fmt.Errorf("Failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendGamesEndOfFile(clientID int) error {
	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, sp.SerializeMsgEndOfFileV2(clientID))
	if err != nil {
		return fmt.Errorf("Failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendReviewsEndOfFile(englishFiltersAmount int, reviewMappersAmount int, clientID int) error {
	for i := 0; i < englishFiltersAmount; i++ {
		err := m.RawReviewsExchange.Publish(RawEnglishReviewsEofKey, sp.SerializeMsgEndOfFileV2(clientID))
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	for i := 0; i < reviewMappersAmount; i++ {
		err := m.RawReviewsExchange.Publish(RawReviewsEofKey, sp.SerializeMsgEndOfFileV2(clientID))
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	return nil
}

func (m *Middleware) ReceiveQueryResponse() ([]byte, error) {
	msg, err := m.QueryResponseQueue.Consume()
	if err != nil {
		return nil, fmt.Errorf("Failed to consume message: %v", err)
	}

	return msg, nil
}
