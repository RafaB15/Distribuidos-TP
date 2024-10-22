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

	IndieReviewJoinExchangeName        = "indie_review_join_exchange"
	IndieReviewJoinExchangeType        = "direct"
	AccumulatedReviewsRoutingKeyPrefix = "accumulated_reviews_key_"
	IndieGameRoutingKeyPrefix          = "indie_key_"
	IndieReviewJoinQueueNamePrefix     = "indie_review_join_queue_"

	TopPositiveReviewsExchangeName = "top_positive_reviews_exchange"
	TopPositiveReviewsExchangeType = "direct"
	TopPositiveReviewsRoutingKey   = "top_positive_reviews_key"
)

type Middleware struct {
	Manager                 *mom.MiddlewareManager
	IndieReviewJoinQueue    *mom.Queue
	PositiveReviewsExchange *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedReviewsRoutingKey := fmt.Sprintf("%s%d", AccumulatedReviewsRoutingKeyPrefix, id)
	indieGameRoutingKey := fmt.Sprintf("%s%d", IndieGameRoutingKeyPrefix, id)
	indieReviewJoinQueueName := fmt.Sprintf("%s%d", IndieReviewJoinQueueNamePrefix, id)

	routingKeys := []string{accumulatedReviewsRoutingKey, indieGameRoutingKey}
	indieReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(indieReviewJoinQueueName, IndieReviewJoinExchangeName, IndieReviewJoinExchangeType, routingKeys, true)
	if err != nil {
		return nil, err
	}

	topPositiveReviewsExchange, err := manager.CreateExchange(TopPositiveReviewsExchangeName, TopPositiveReviewsExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                 manager,
		IndieReviewJoinQueue:    indieReviewJoinQueue,
		PositiveReviewsExchange: topPositiveReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveMsg() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error) {
	rawMsg, err := m.IndieReviewJoinQueue.Consume()
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

func (m *Middleware) SendMetrics(reviewsInformation *j.JoinedActionGameReview) error {
	serializedMetrics, err := sp.SerializeMsgJoinedActionGameReviews(reviewsInformation)
	if err != nil {
		return err
	}

	return m.PositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, serializedMetrics)
}

func (m *Middleware) SendEof() error {
	err := m.PositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		return err
	}

	return nil
}
