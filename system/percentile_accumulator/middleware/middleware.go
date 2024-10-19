package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	StoredReviewsFileName = "stored_reviews"

	AccumulatedReviewsExchangeName = "accumulated_reviews_exchange"
	AccumulatedReviewsExchangeType = "direct"
	AccumulatedReviewsRoutingKey   = "accumulated_reviews_key"
	AccumulatedReviewsQueueName    = "accumulated_reviews_queue"

	AccumulatedPercentileReviewsExchangeName     = "action_review_join_exchange"
	AccumulatedPercentileReviewsExchangeType     = "direct"
	AccumulatedPercentileReviewsRoutingKeyPrefix = "percentile_reviews_key_"

	ActionNegativeReviewsJoinersAmountEnvironmentVariableName = "ACTION_NEGATIVE_REVIEWS_JOINERS_AMOUNT"

	IdEnvironmentVariableName = "ID"
	NumPreviousAccumulators   = "NUM_PREVIOUS_ACCUMULATORS"
)

type Middleware struct {
	Manager                       *mom.MiddlewareManager
	AccumulatedReviewsQueue       *mom.Queue
	AccumulatedPercentileExchange *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {

	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}
	accumulatedReviewsQueue, err := manager.CreateBoundQueue(AccumulatedReviewsQueueName, AccumulatedReviewsExchangeName, AccumulatedReviewsExchangeType, AccumulatedReviewsRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}
	accumulatedPercentileExchange, err := manager.CreateExchange(AccumulatedPercentileReviewsExchangeName, AccumulatedPercentileReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}
	return &Middleware{
		Manager:                       manager,
		AccumulatedReviewsQueue:       accumulatedReviewsQueue,
		AccumulatedPercentileExchange: accumulatedPercentileExchange,
	}, nil

}

func (m *Middleware) ReceiveGameReviewsMetrics() ([]*ra.GameReviewsMetrics, bool, error) {
	msg, err := m.AccumulatedReviewsQueue.Consume()
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

func (m *Middleware) SendGameReviewsMetrics(accumulatedPercentileKeyMap map[string][]*ra.GameReviewsMetrics) error {
	for routingKey, metrics := range accumulatedPercentileKeyMap {
		serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(metrics)

		err := m.AccumulatedPercentileExchange.Publish(routingKey, serializedMetricsBatch)
		if err != nil {
			return fmt.Errorf("Failed to publish metrics: %v", err)
		}
	}
	return nil
}

func (m *Middleware) SendEndOfFiles(actionNegativeReviewsJoinersAmount int, accumulatedPercentileReviewsRoutingKeyPrefix string) error {
	for i := 1; i <= actionNegativeReviewsJoinersAmount; i++ {
		routingKey := fmt.Sprintf("%v%d", accumulatedPercentileReviewsRoutingKeyPrefix, i)
		err := m.AccumulatedPercentileExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
		if err != nil {
			return err
		}
	}
	return nil
}
