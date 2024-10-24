package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	FinalPositiveJoinerExchangeName = "final_positive_joiner_exchange"
	FinalPositiveJoinerRoutingKey   = "final_positive_joiner_key"
	FinalPositiveJoinerExchangeType = "direct"
	FinalPositiveJoinerQueueName    = "final_positive_joiner_queue"

	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager                  *mom.MiddlewareManager
	FinalPositiveJoinerQueue *mom.Queue
	QueryResultsExchange     *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	finalPositiveJoinerQueue, err := manager.CreateBoundQueue(FinalPositiveJoinerQueueName, FinalPositiveJoinerExchangeName, FinalPositiveJoinerExchangeType, FinalPositiveJoinerRoutingKey, true)
	if err != nil {
		return nil, err
	}

	queryResultsExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                  manager,
		FinalPositiveJoinerQueue: finalPositiveJoinerQueue,
		QueryResultsExchange:     queryResultsExchange,
	}, nil
}

func (m *Middleware) ReceiveJoinedGameReviews() (int, *j.JoinedPositiveGameReview, bool, error) {
	rawMsg, err := m.FinalPositiveJoinerQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, err
	}

	switch message.Type {
	case sp.MsgJoinedPositiveGameReviews:
		joinedPositiveGameReview, err := sp.DeserializeMsgJoinedPositiveGameReviewsV2(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}
		return message.ClientID, joinedPositiveGameReview, false, nil

	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil

	default:
		return message.ClientID, nil, false, fmt.Errorf("Received unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendQueryResults(clientID int, queryResults []*j.JoinedPositiveGameReview) error {
	queryMessage, err := sp.SerializeMsgActionPositiveReviewsQuery(clientID, queryResults)
	if err != nil {
		return err
	}

	routingKey := QueryRoutingKeyPrefix + fmt.Sprint(clientID)
	return m.QueryResultsExchange.Publish(routingKey, queryMessage)
}

func (m *Middleware) SendEof(clientID int) error {
	routingKey := QueryRoutingKeyPrefix + fmt.Sprint(clientID)
	err := m.QueryResultsExchange.Publish(routingKey, sp.SerializeMsgEndOfFileV2(clientID))
	if err != nil {
		return err
	}
	return nil
}
