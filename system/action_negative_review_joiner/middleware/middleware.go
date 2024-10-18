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

func (m *Middleware) ReceiveMsg() (sp.MessageType, []byte, bool, error) {
	msg, err := m.ActionReviewJoinQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	messageType, err := sp.DeserializeMessageType(msg)
	if err != nil {
		return 0, nil, false, err
	}

	if messageType == sp.MsgEndOfFile {
		return messageType, nil, true, nil
	}

	return messageType, msg, false, nil

}

func (m *Middleware) SendMetrics(reviewsInformation *j.JoinedActionNegativeGameReview) error {
	serializedMetrics, err := sp.SerializeMsgNegativeJoinedActionGameReviews(reviewsInformation)
	if err != nil {
		return err
	}

	return m.WriterExchange.Publish(WriterRoutingKey, serializedMetrics)
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

func (m *Middleware) SendEof() error {
	err := m.WriterExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		return err
	}

	return nil
}
