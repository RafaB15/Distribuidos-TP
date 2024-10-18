package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	AccumulatedEnglishReviewsExchangeName = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType = "direct"
	AccumulatedEnglishReviewsRoutingKey   = "accumulated_english_reviews_key"
	AccumulatedEnglishReviewQueueName     = "accumulated_english_reviews_queue"

	PositiveJoinReviewsExchangeName     = "action_review_join_exchange"
	PositiveJoinReviewsExchangeType     = "direct"
	PositiveJoinReviewsRoutingKeyPrefix = "positive_reviews_key_"
)

type Middleware struct {
	Manager                        *mom.MiddlewareManager
	AccumulatedEnglishReviewsQueue *mom.Queue
	PositiveJoinedReviewsExchange  *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedEnglishReviewsQueue, err := manager.CreateBoundQueue(AccumulatedEnglishReviewQueueName, AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType, AccumulatedEnglishReviewsRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	positiveJoinedEnglishReviewsExchange, err := manager.CreateExchange(PositiveJoinReviewsExchangeName, PositiveJoinReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                        manager,
		AccumulatedEnglishReviewsQueue: accumulatedEnglishReviewsQueue,
		PositiveJoinedReviewsExchange:  positiveJoinedEnglishReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveGameReviewsMetrics() ([]*ra.GameReviewsMetrics, bool, error) {
	msg, err := m.AccumulatedEnglishReviewsQueue.Consume()
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
	case sp.MsgGameReviewsMetrics:
		gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(msg)
		if err != nil {
			return nil, false, err
		}
		return gameReviewsMetrics, false, nil
	default:
		return nil, false, fmt.Errorf("Received unexpected message type: %v", messageType)
	}
}

func (m *Middleware) SendGameReviewsMetrics(positiveReviewsMap map[int][]*ra.GameReviewsMetrics) error {
	for shardingKey, gameReviewsMetrics := range positiveReviewsMap {
		routingKey := fmt.Sprintf("%s%d", PositiveJoinReviewsRoutingKeyPrefix, shardingKey)
		serializedGameReviewsMetrics := sp.SerializeMsgGameReviewsMetricsBatch(gameReviewsMetrics)

		err := m.PositiveJoinedReviewsExchange.Publish(routingKey, serializedGameReviewsMetrics)
		if err != nil {
			return fmt.Errorf("Failed to publish game reviews metrics: %v", err)
		}
	}
	return nil
}

func (m *Middleware) SendEndOfFiles(actionReviewsJoinersAmount int) error {
	for i := 1; i <= actionReviewsJoinersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", PositiveJoinReviewsRoutingKeyPrefix, i)
		m.PositiveJoinedReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
	}
	return nil
}
