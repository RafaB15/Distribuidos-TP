package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	j "distribuidos-tp/internal/system_protocol/joiner"
	mom "distribuidos-tp/middleware"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	TopPositiveReviewsExchangeName = "top_positive_reviews_exchange"
	TopPositiveReviewsEchangeType  = "direct"
	TopPositiveReviewsRoutingKey   = "top_positive_reviews_key"
	TopPositiveReviewsQueueName    = "top_positive_reviews_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"
)

type Middleware struct {
	Manager                 *mom.MiddlewareManager
	TopPositiveReviewsQueue *mom.Queue
	WriterExchange          *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	topPositiveReviewsQueue, err := manager.CreateBoundQueue(TopPositiveReviewsQueueName, TopPositiveReviewsExchangeName, TopPositiveReviewsEchangeType, TopPositiveReviewsRoutingKey, true)
	if err != nil {
		return nil, err
	}

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                 manager,
		TopPositiveReviewsQueue: topPositiveReviewsQueue,
		WriterExchange:          writerExchange,
	}, nil
}

func (m *Middleware) ReceiveMsg() (*j.JoinedActionGameReview, bool, error) {
	msg, err := m.TopPositiveReviewsQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	messageType, err := sp.DeserializeMessageType(msg)
	if err != nil {
		return nil, false, err
	}

	switch messageType {
	case sp.MsgEndOfFile:
		return nil, true, nil

	case sp.MsgQueryResolved:
		joinedGame, err := sp.DeserializeMsgJoinedActionGameReviews(msg)
		if err != nil {
			return nil, false, err
		}

		return joinedGame, false, nil

	default:
		return nil, false, nil
	}
}

func (m *Middleware) SendMetrics(topPositiveIndieGames []*j.JoinedActionGameReview) error {
	data := sp.SerializeMsgJoinedIndieGameReviewsBatch(topPositiveIndieGames)

	err := m.WriterExchange.Publish(WriterRoutingKey, data)
	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) SendEof() error {
	return m.WriterExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
}
