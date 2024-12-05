package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	mom "distribuidos-tp/middleware"

	"github.com/op/go-logging"

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
	logger                  *logging.Logger
}

func NewMiddleware(logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedReviewsQueue, err := manager.CreateBoundQueue(AccumulatedReviewsQueueName, AccumulatedReviewsExchangeName, AccumulatedReviewsExchangeType, AccumulatedReviewsRoutingKey, false)
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
		logger:                  logger,
	}, nil

}

func (m *Middleware) ReceiveGameReviewsMetrics(messageTracker *n.MessageTracker) (clientID int, namedReviews []*ra.NamedGameReviewsMetrics, eof bool, newMessage bool, delMessage bool, e error) {
	rawMsg, err := m.AccumulatedReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, false, false, err
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return 0, nil, false, false, false, err
	}

	if !newMessage {
		return message.ClientID, nil, false, false, false, nil
	}

	switch message.Type {
	case sp.MsgEndOfFile:

		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, err
		}
		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, false, err
		}
		return message.ClientID, nil, true, true, false, nil

	case sp.MsgDeleteClient:
		m.logger.Infof("Receive delete client %d", message.ClientID)
		return message.ClientID, nil, false, true, true, nil
	case sp.MsgNamedGameReviewsMetrics:

		gameReviewsMetrics, err := sp.DeserializeMsgNamedGameReviewsMetricsBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, err
		}

		return message.ClientID, gameReviewsMetrics, false, true, false, nil

	default:

		return message.ClientID, nil, false, false, false, fmt.Errorf("received unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendQueryResults(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error {
	queryMessage := sp.SerializeMsgActionNegativeReviewsQuery(clientID, namedGameReviewsMetricsBatch)
	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	return m.QueryResultsExchange.Publish(routingKey, queryMessage)
}

func (m *Middleware) AckLastMessage() error {
	err := m.AccumulatedReviewsQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
