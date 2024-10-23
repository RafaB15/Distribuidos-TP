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

	// lo que recibo del percentil 90
	ActionPercentileReviewJoinExchangeName     = "action_review_join_exchange"
	ActionPercentileReviewJoinExchangeType     = "direct"
	ActionPercentileReviewJoinRoutingKeyPrefix = "percentile_reviews_key_"

	// lo que recibo del game mapper
	ActionGameRoutingKeyPrefix = "action_key_"

	ActionNegativeReviewJoinQueueNamePrefix = "action_negative_review_join_queue_"

	// lo que voy a estar mandando al writer (esto deberia estar ok porque es igual a todos los otros)
	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"
)

type Middleware struct {
	Manager               *mom.MiddlewareManager
	ActionReviewJoinQueue *mom.Queue
	WriterExchange        *mom.Exchange
}

func NewMiddleware(id string) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	actionReviewJoinRoutingKey := fmt.Sprintf("%s%s", ActionPercentileReviewJoinRoutingKeyPrefix, id)
	actionGameRoutingKey := fmt.Sprintf("%s%s", ActionGameRoutingKeyPrefix, id)

	routingKeys := []string{actionReviewJoinRoutingKey, actionGameRoutingKey}
	actionReviewJoinQueueName := fmt.Sprintf("%s%s", ActionNegativeReviewJoinQueueNamePrefix, id)
	actionReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(actionReviewJoinQueueName, ActionPercentileReviewJoinExchangeName, ActionPercentileReviewJoinExchangeType, routingKeys, true)
	if err != nil {
		return nil, err
	}

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:               manager,
		ActionReviewJoinQueue: actionReviewJoinQueue,
		WriterExchange:        writerExchange,
	}, nil
}

func (m *Middleware) ReceiveMsg() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error) {
	rawMsg, err := m.ActionReviewJoinQueue.Consume()
	if err != nil {
		return nil, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return nil, nil, false, err
	}

	switch message.Type {
	case sp.MsgGameNames:
		games, err := HandleGameNames(message.Body)
		if err != nil {
			return nil, nil, false, err
		}
		return games, nil, false, nil
	case sp.MsgGameReviewsMetrics:
		reviews, err := HandleGameReviewMetrics(message.Body)
		if err != nil {
			return nil, nil, false, err
		}
		return nil, reviews, false, err

	case sp.MsgEndOfFile:
		return nil, nil, true, nil

	default:
		return nil, nil, false, fmt.Errorf("Unknown type msg")
	}

}

func (m *Middleware) SendMetrics(reviewsInformation *j.JoinedNegativeGameReview) error {
	serializedMetrics, err := sp.SerializeMsgNegativeJoinedPositiveGameReviews(reviewsInformation)
	if err != nil {
		return err
	}

	return m.WriterExchange.Publish(WriterRoutingKey, serializedMetrics)
}

func HandleGameReviewMetrics(message []byte) ([]*reviews_accumulator.GameReviewsMetrics, error) {
	reviews, err := sp.DeserializeMsgGameReviewsMetricsBatchV2(message)
	if err != nil {
		return nil, err
	}
	return reviews, err
}

func HandleGameNames(message []byte) ([]*games.GameName, error) {
	gameNames, err := sp.DeserializeMsgGameNamesV2(message)
	if err != nil {
		return nil, err
	}
	return gameNames, nil
}

func (m *Middleware) SendEof() error {
	err := m.WriterExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		return err
	}

	return nil
}
