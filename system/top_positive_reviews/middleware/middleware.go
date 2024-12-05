package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	mom "distribuidos-tp/middleware"
	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	TopPositiveReviewsExchangeName = "top_positive_reviews_exchange"
	TopPositiveReviewsExchangeType = "direct"
	TopPositiveReviewsRoutingKey   = "top_positive_reviews_key"
	TopPositiveReviewsQueueName    = "top_positive_reviews_queue"

	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_"
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager                 *mom.MiddlewareManager
	TopPositiveReviewsQueue *mom.Queue
	QueryResultsExchange    *mom.Exchange
	logger                  *logging.Logger
}

func NewMiddleware(logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	topPositiveReviewsQueue, err := manager.CreateBoundQueue(TopPositiveReviewsQueueName, TopPositiveReviewsExchangeName, TopPositiveReviewsExchangeType, TopPositiveReviewsRoutingKey, false)
	if err != nil {
		return nil, err
	}

	queryResultsExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                 manager,
		TopPositiveReviewsQueue: topPositiveReviewsQueue,
		QueryResultsExchange:    queryResultsExchange,
		logger:                  logger,
	}, nil
}

func (m *Middleware) ReceiveMsg(messageTracker *n.MessageTracker) (clientID int, reviews *j.JoinedPositiveGameReview, eof bool, newMessage bool, delMessage bool, e error) {
	rawMsg, err := m.TopPositiveReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, false, false, err
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return message.ClientID, nil, false, false, false, nil
	}

	if !newMessage {
		return message.ClientID, nil, false, false, false, nil
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, fmt.Errorf("failed to deserialize EOF message: %v", err)
		}
		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, false, fmt.Errorf("failed to register EOF: %v", err)
		}
		return message.ClientID, nil, true, true, false, nil

	case sp.MsgDeleteClient:
		m.logger.Infof("Received Delete Client for client %d", message.ClientID)
		return message.ClientID, nil, false, true, true, nil

	case sp.MsgJoinedPositiveGameReviews:
		joinedGame, err := sp.DeserializeMsgJoinedPositiveGameReviews(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, err
		}

		return message.ClientID, joinedGame, false, true, false, nil

	default:
		return message.ClientID, nil, false, false, false, nil
	}
}

func (m *Middleware) SendQueryResults(clientID int, topPositiveIndieGames []*j.JoinedPositiveGameReview) error {
	queryMessage, err := sp.SerializeMsgIndiePositiveJoinedReviewsQuery(clientID, topPositiveIndieGames)
	if err != nil {
		return err
	}
	routingKey := QueryRoutingKeyPrefix + fmt.Sprint(clientID)
	return m.QueryResultsExchange.Publish(routingKey, queryMessage)

}

func (m *Middleware) AckLastMessage() error {
	err := m.TopPositiveReviewsQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
