package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	AccumulatedEnglishReviewsExchangeName = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType = "direct"
	AccumulatedEnglishReviewsRoutingKey   = "accumulated_english_reviews_key"
	AccumulatedEnglishReviewQueueName     = "accumulated_english_reviews_queue"

	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager                        *mom.MiddlewareManager
	AccumulatedEnglishReviewsQueue *mom.Queue
	QueryResultsExchange           *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedEnglishReviewsQueue, err := manager.CreateBoundQueue(AccumulatedEnglishReviewQueueName, AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType, AccumulatedEnglishReviewsRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	queryResultsExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                        manager,
		AccumulatedEnglishReviewsQueue: accumulatedEnglishReviewsQueue,
		QueryResultsExchange:           queryResultsExchange,
	}, nil
}

func (m *Middleware) ReceiveGameReviewsMetrics() (clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics, eof bool, err error) {
	rawMsg, err := m.AccumulatedEnglishReviewsQueue.Consume()
	if err != nil {
		return
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		err = fmt.Errorf("failed to deserialize message: %v", err)
		return
	}

	clientID = message.ClientID

	switch message.Type {
	case sp.MsgEndOfFile:
		eof = true
	case sp.MsgNamedGameReviewsMetrics:
		namedGameReviewsMetricsBatch, err = sp.DeserializeMsgNamedGameReviewsMetricsBatch(message.Body)
	default:
		err = fmt.Errorf("received unexpected message type: %v", message.Type)
	}
	return
}

func (m *Middleware) SendQueryResults(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error {
	queryMessage := sp.SerializeMsgActionNegativeEnglishReviewsQuery(clientID, namedGameReviewsMetricsBatch)
	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	return m.QueryResultsExchange.Publish(routingKey, queryMessage)
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
