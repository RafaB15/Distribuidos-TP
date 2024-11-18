package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	AccumulatedEnglishReviewsExchangeName     = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType     = "direct"
	AccumulatedEnglishReviewsRoutingKeyPrefix = "accumulated_english_reviews_key_"
	AccumulatedEnglishReviewQueueNamePrefix   = "accumulated_english_reviews_queue_"

	PositiveJoinReviewsExchangeName     = "action_review_join_exchange"
	PositiveJoinReviewsExchangeType     = "direct"
	PositiveJoinReviewsRoutingKeyPrefix = "positive_reviews_key_"
)

type Middleware struct {
	Manager                        *mom.MiddlewareManager
	AccumulatedEnglishReviewsQueue *mom.Queue
	PositiveJoinedReviewsExchange  *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedEnglishReviewRoutingKey := fmt.Sprintf("%s%d", AccumulatedEnglishReviewsRoutingKeyPrefix, id)
	accumulatedEnglishReviewQueueName := fmt.Sprintf("%s%d", AccumulatedEnglishReviewQueueNamePrefix, id)
	accumulatedEnglishReviewsQueue, err := manager.CreateBoundQueue(accumulatedEnglishReviewQueueName, AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType, accumulatedEnglishReviewRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	positiveJoinedEnglishReviewsExchange, err := manager.CreateExchange(PositiveJoinReviewsExchangeName, PositiveJoinReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                        manager,
		AccumulatedEnglishReviewsQueue: accumulatedEnglishReviewsQueue,
		PositiveJoinedReviewsExchange:  positiveJoinedEnglishReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveGameReviewsMetrics() (int, []*ra.GameReviewsMetrics, bool, error) {
	rawMsg, err := m.AccumulatedEnglishReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	case sp.MsgGameReviewsMetrics:
		gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}
		return message.ClientID, gameReviewsMetrics, false, nil
	default:
		return message.ClientID, nil, false, fmt.Errorf("received unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendGameReviewsMetrics(clientID int, positiveReviewsMap map[int][]*ra.GameReviewsMetrics) error {
	for shardingKey, gameReviewsMetrics := range positiveReviewsMap {
		routingKey := fmt.Sprintf("%s%d", PositiveJoinReviewsRoutingKeyPrefix, shardingKey)
		serializedGameReviewsMetrics := sp.SerializeMsgGameReviewsMetricsBatch(clientID, gameReviewsMetrics)
		err := m.PositiveJoinedReviewsExchange.Publish(routingKey, serializedGameReviewsMetrics)
		if err != nil {
			return fmt.Errorf("failed to publish game reviews metrics: %v", err)
		}
	}
	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int, actionReviewsJoinersAmount int) error {
	for i := 1; i <= actionReviewsJoinersAmount; i++ {
		serializedEOF := sp.SerializeMsgEndOfFile(clientID)
		routingKey := fmt.Sprintf("%s%d", PositiveJoinReviewsRoutingKeyPrefix, i)
		err := m.PositiveJoinedReviewsExchange.Publish(routingKey, serializedEOF)
		if err != nil {
			return fmt.Errorf("failed to publish end of file: %v", err)
		}
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
