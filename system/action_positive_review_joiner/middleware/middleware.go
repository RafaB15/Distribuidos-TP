package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"
	mom "distribuidos-tp/middleware"

	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	ActionReviewJoinExchangeName     = "action_review_join_exchange"
	ActionReviewJoinExchangeType     = "direct"
	ActionReviewJoinRoutingKeyPrefix = "positive_reviews_key_"
	ActionGameRoutingKeyPrefix       = "action_key_"
	ActionReviewJoinQueueNamePrefix  = "action_review_join_queue_"

	FinalPositiveJoinerExchangeName = "final_positive_joiner_exchange"
	FinalPositiveJoinerRoutingKey   = "final_positive_joiner_key"
	FinalPositiveJoinerExchangeType = "direct"
)

type Middleware struct {
	Manager                     *mom.MiddlewareManager
	ActionReviewJoinQueue       *mom.Queue
	FinalPositiveJoinerExchange *mom.Exchange
}

func NewMiddleware(id string) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	actionReviewJoinRoutingKey := fmt.Sprintf("%s%s", ActionReviewJoinRoutingKeyPrefix, id)
	actionGameRoutingKey := fmt.Sprintf("%s%s", ActionGameRoutingKeyPrefix, id)

	routingKeys := []string{actionReviewJoinRoutingKey, actionGameRoutingKey}
	actionReviewJoinQueueName := ActionReviewJoinQueueNamePrefix + string(id)
	actionReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(actionReviewJoinQueueName, ActionReviewJoinExchangeName, ActionReviewJoinExchangeType, routingKeys, true)
	if err != nil {
		return nil, err
	}

	finalPositiveJoinerExchange, err := manager.CreateExchange(FinalPositiveJoinerExchangeName, FinalPositiveJoinerExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                     manager,
		ActionReviewJoinQueue:       actionReviewJoinQueue,
		FinalPositiveJoinerExchange: finalPositiveJoinerExchange,
	}, nil
}

func (m *Middleware) ReceiveMsg() (int, []*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error) {
	rawMsg, err := m.ActionReviewJoinQueue.Consume()
	if err != nil {
		return 0, nil, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, nil, false, err
	}

	switch message.Type {
	case sp.MsgGameNames:
		games, err := HandleGameNames(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, err
		}
		return message.ClientID, games, nil, false, nil
	case sp.MsgGameReviewsMetrics:
		reviews, err := HandleGameReviewMetrics(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, err
		}
		return message.ClientID, nil, reviews, false, err

	case sp.MsgEndOfFile:
		return message.ClientID, nil, nil, true, nil

	default:
		return message.ClientID, nil, nil, false, fmt.Errorf("Unknown type msg")
	}

}

func (m *Middleware) SendMetrics(clientID int, reviewsInformation *j.JoinedPositiveGameReview) error {
	serializedMetrics, err := sp.SerializeMsgJoinedPositiveGameReviews(clientID, reviewsInformation)
	if err != nil {
		return err
	}

	return m.FinalPositiveJoinerExchange.Publish(FinalPositiveJoinerRoutingKey, serializedMetrics)
}

func HandleGameReviewMetrics(message []byte) ([]*reviews_accumulator.GameReviewsMetrics, error) {
	reviews, err := sp.DeserializeMsgGameReviewsMetricsBatch(message)
	if err != nil {
		return nil, err
	}
	return reviews, err
}

func HandleGameNames(message []byte) ([]*games.GameName, error) {
	gameNames, err := sp.DeserializeMsgGameNames(message)
	if err != nil {
		return nil, err
	}
	return gameNames, nil
}

func (m *Middleware) SendEof(clientID int) error {
	err := m.FinalPositiveJoinerExchange.Publish(FinalPositiveJoinerRoutingKey, sp.SerializeMsgEndOfFile(clientID))
	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
