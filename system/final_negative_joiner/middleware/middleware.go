package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	FinalNegativeJoinerExchangeName = "final_negative_joiner_exchange"
	FinalNegativeJoinerRoutingKey   = "final_negative_joiner_key"
	FinalNegativeJoinerExchangeType = "direct"
	FinalNegativeJoinerQueueName    = "final_negative_joiner_queue"

	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager                  *mom.MiddlewareManager
	FinalNegativeJoinerQueue *mom.Queue
	QueryResultsExchange     *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	finalNegativeJoinerQueue, err := manager.CreateBoundQueue(FinalNegativeJoinerQueueName, FinalNegativeJoinerExchangeName, FinalNegativeJoinerExchangeType, FinalNegativeJoinerRoutingKey, true)
	if err != nil {
		return nil, err
	}

	queryResultsExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                  manager,
		FinalNegativeJoinerQueue: finalNegativeJoinerQueue,
		QueryResultsExchange:     queryResultsExchange,
	}, nil
}

func (m *Middleware) ReceiveJoinedGameReviews() (int, *j.JoinedNegativeGameReview, bool, error) {
	rawMsg, err := m.FinalNegativeJoinerQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, err
	}

	switch message.Type {
	case sp.MsgJoinedNegativeGameReviews:
		joinedNegativeGameReview, err := sp.DeserializeMsgNegativeJoinedPositiveGameReviewsV2(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}
		return message.ClientID, joinedNegativeGameReview, false, nil

	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	default:
		return message.ClientID, nil, false, fmt.Errorf("Unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendQueryResults(clientID int, queryResults []*j.JoinedNegativeGameReview) error {
	queryMessage, err := sp.SerializeMsgActionNegativeReviewsQuery(clientID, queryResults)
	if err != nil {
		return err
	}

	routingKey := QueryRoutingKeyPrefix + fmt.Sprint(clientID)
	return m.QueryResultsExchange.Publish(routingKey, queryMessage)
}
