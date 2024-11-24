package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI                  = "amqp://guest:guest@rabbitmq:5672/"
	AccumulatedReviewsExchangeName = "accumulated_reviews_exchange"
	AccumulatedReviewsExchangeType = "direct"
	AccumulatedReviewsRoutingKey   = "accumulated_reviews_key"
	AccumulatedReviewsQueueName    = "accumulated_reviews_queue"

	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager                 *mom.MiddlewareManager
	AccumulatedReviewsQueue *mom.Queue
	QueryResultsExchange    *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedReviewsQueue, err := manager.CreateBoundQueue(AccumulatedReviewsQueueName, AccumulatedReviewsExchangeName, AccumulatedReviewsExchangeType, AccumulatedReviewsRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	queryResultsExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                 manager,
		AccumulatedReviewsQueue: accumulatedReviewsQueue,
		QueryResultsExchange:    queryResultsExchange,
	}, nil

}

func (m *Middleware) ReceiveGameReviewsMetrics() (int, []*ra.NamedGameReviewsMetrics, bool, error) {
	rawMsg, err := m.AccumulatedReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, err
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		fmt.Printf("Message body: %v\n", message.Body)
		return message.ClientID, nil, true, nil
	case sp.MsgNamedGameReviewsMetrics:
		fmt.Print("Received game reviews metrics\n")
		gameReviewsMetrics, err := sp.DeserializeMsgNamedGameReviewsMetricsBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}
		fmt.Printf("Message: %v\n", message.Body)
		return message.ClientID, gameReviewsMetrics, false, nil
	default:
		fmt.Printf("Received unexpected message type: %v\n", message.Type)
		fmt.Printf("Message body: %v\n", rawMsg)
		return message.ClientID, nil, false, fmt.Errorf("received unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendQueryResults(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error {
	queryMessage := sp.SerializeMsgActionNegativeReviewsQuery(clientID, namedGameReviewsMetricsBatch)
	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	return m.QueryResultsExchange.Publish(routingKey, queryMessage)
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
