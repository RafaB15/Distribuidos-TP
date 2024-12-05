package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	mom "distribuidos-tp/middleware"
	"fmt"

	"github.com/op/go-logging"
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
	logger                         *logging.Logger
}

func NewMiddleware(logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedEnglishReviewsQueue, err := manager.CreateBoundQueue(AccumulatedEnglishReviewQueueName, AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType, AccumulatedEnglishReviewsRoutingKey, false)
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
		logger:                         logger,
	}, nil
}

func (m *Middleware) ReceiveGameReviewsMetrics(messageTracker *n.MessageTracker) (clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics, eof bool, newMessage bool, delMessage bool, e error) {
	rawMsg, err := m.AccumulatedEnglishReviewsQueue.Consume()
	if err != nil {
		e = fmt.Errorf("failed to consume message: %v", err)
		return
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		e = fmt.Errorf("failed to deserialize message: %v", err)
		return
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		e = fmt.Errorf("failed to process message: %v", err)
		return
	}

	delMessage = false
	clientID = message.ClientID

	if !newMessage {
		return
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		eof = true
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			e = fmt.Errorf("failed to deserialize EOF message: %v", err)
			return
		}
		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			e = fmt.Errorf("failed to register EOF: %v", err)
			return
		}
	case sp.MsgDeleteClient:
		m.logger.Infof("Received Delete Client Message. Deleting client %d", message.ClientID)
		delMessage = true
		return
	case sp.MsgNamedGameReviewsMetrics:
		namedGameReviewsMetricsBatch, e = sp.DeserializeMsgNamedGameReviewsMetricsBatch(message.Body)
	default:
		e = fmt.Errorf("received unexpected message type: %v", message.Type)
	}
	return
}

func (m *Middleware) SendQueryResults(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error {
	queryMessage := sp.SerializeMsgActionNegativeEnglishReviewsQuery(clientID, namedGameReviewsMetricsBatch)
	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	return m.QueryResultsExchange.Publish(routingKey, queryMessage)
}

func (m *Middleware) AckLastMessage() error {
	err := m.AccumulatedEnglishReviewsQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
