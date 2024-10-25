package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
	mom "distribuidos-tp/middleware"
	"fmt"
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
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	topPositiveReviewsQueue, err := manager.CreateBoundQueue(TopPositiveReviewsQueueName, TopPositiveReviewsExchangeName, TopPositiveReviewsExchangeType, TopPositiveReviewsRoutingKey, true)
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
	}, nil
}

func (m *Middleware) ReceiveMsg() (int, *j.JoinedPositiveGameReview, bool, error) {
	rawMsg, err := m.TopPositiveReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, err
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil

	case sp.MsgJoinedPositiveGameReviews:
		joinedGame, err := sp.DeserializeMsgJoinedPositiveGameReviews(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}

		return message.ClientID, joinedGame, false, nil

	default:
		return message.ClientID, nil, false, nil
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
