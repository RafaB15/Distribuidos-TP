package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	j "distribuidos-tp/internal/system_protocol/joiner"
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

	QueryResultsQueueName    = "query_results_queue"
	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager            *mom.MiddlewareManager
	RawGamesExchange   *mom.Exchange
	RawReviewsExchange *mom.Exchange
	QueryResultsQueue  *mom.Queue
}

func NewMiddleware(clientID int) (*Middleware, error) {
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

	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	queryResultsQueue, err := manager.CreateBoundQueue(QueryResultsQueueName, QueryResultsExchangeName, QueryExchangeType, routingKey, true)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	return &Middleware{
		Manager:            manager,
		RawGamesExchange:   rawGamesExchange,
		RawReviewsExchange: rawReviewsExchange,
		QueryResultsQueue:  queryResultsQueue,
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

func (m *Middleware) SendReviewsEndOfFile(clientID int, englishFiltersAmount int, reviewMappersAmount int) error {
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
	rawMsg, err := m.QueryResultsQueue.Consume()
	if err != nil {
		return nil, fmt.Errorf("Failed to consume message: %v", err)
	}

	queryResponseMessage, err := sp.DeserializeQuery(rawMsg)

	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize message: %v", err)
	}

	switch queryResponseMessage.Type {
	case sp.MsgOsResolvedQuery:
		fmt.Printf("Received OS resolved query\n")
		return handleMsgOsResolvedQuery(queryResponseMessage.Body)
	case sp.MsgActionPositiveReviewsQuery:
		fmt.Printf("Received positive reviews query\n")
		return handleMsgActionPositiveReviewsQuery(queryResponseMessage.Body)
	}

	return rawMsg, nil
}

func handleMsgOsResolvedQuery(message []byte) ([]byte, error) {
	gameOSMetrics, err := sp.DeserializeMsgOsResolvedQuery(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize message: %v", err)
	}
	stringRepresentation := oa.GetStrRepresentation(gameOSMetrics)

	return sp.AssembleFinalQueryMsg(byte(sp.MsgOsResolvedQuery), []byte(stringRepresentation)), nil
}

func handleMsgActionPositiveReviewsQuery(message []byte) ([]byte, error) {
	joinedReviews, err := sp.DeserializeMsgActionPositiveReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize message: %v", err)
	}

	var stringRepresentation []byte
	for _, review := range joinedReviews {
		stringRep := j.GetStrRepresentation(review)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(sp.MsgActionPositiveReviewsQuery), []byte(stringRepresentation)), nil
}
