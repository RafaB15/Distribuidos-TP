package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
	"strconv"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"
	ReviewQueueNamePrefix   = "reviews_queue_"

	AccumulatedReviewsExchangeName = "accumulated_reviews_exchange"
	AccumulatedReviewsExchangeType = "direct"
	AccumulatedReviewsRoutingKey   = "accumulated_reviews_key"

	IndieReviewJoinExchangeName             = "indie_review_join_exchange"
	IndieReviewJoinExchangeType             = "direct"
	IndieReviewJoinExchangeRoutingKeyPrefix = "accumulated_reviews_key_"

	IdEnvironmentVariableName    = "ID"
	IndieReviewJoinersAmountName = "INDIE_REVIEW_JOINERS_AMOUNT"
)

type Middleware struct {
	Manager                    *mom.MiddlewareManager
	ReviewsQueue               *mom.Queue
	AccumulatedReviewsExchange *mom.Exchange
	IndieReviewJoinExchange    *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	id, err := u.GetEnv(IdEnvironmentVariableName)
	if err != nil {
		return nil, err
	}

	reviewQueueName := fmt.Sprintf("%s%s", ReviewQueueNamePrefix, id)
	reviewsRoutingKey := fmt.Sprintf("%s%s", ReviewsRoutingKeyPrefix, id)
	reviewsQueue, err := manager.CreateBoundQueue(reviewQueueName, ReviewsExchangeName, ReviewsExchangeType, reviewsRoutingKey, true)
	if err != nil {
		return nil, err
	}

	accumulatedReviewsExchange, err := manager.CreateExchange(AccumulatedReviewsExchangeName, AccumulatedReviewsExchangeType)
	if err != nil {
		return nil, err
	}

	indieReviewJoinExchange, err := manager.CreateExchange(IndieReviewJoinExchangeName, IndieReviewJoinExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                    manager,
		ReviewsQueue:               reviewsQueue,
		AccumulatedReviewsExchange: accumulatedReviewsExchange,
		IndieReviewJoinExchange:    indieReviewJoinExchange,
	}, nil
}

func (m *Middleware) ReceiveReviews() ([]*reviews.Review, bool, error) {
	rawMsg, err := m.ReviewsQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return nil, false, fmt.Errorf("Failed to deserialize message: %v", err)
	}

	fmt.Printf("Received message from client %d\n", message.ClientID)

	switch message.MessageType {
	case sp.MsgEndOfFile:
		return nil, true, nil
	case sp.MsgReviewInformation:
		reviews, err := sp.DeserializeMsgReviewInformationV2(message.Body)
		if err != nil {
			return nil, false, fmt.Errorf("Failed to deserialize reviews: %v", err)
		}
		return reviews, false, nil
	default:
		return nil, false, fmt.Errorf("Unexpected message type: %v", message.MessageType)
	}
}

func (m *Middleware) SendAccumulatedReviews(accumulatedReviews map[uint32]*r.GameReviewsMetrics) error {
	keyMap := idMapToKeyMap(accumulatedReviews, GetIndieReviewJoinersAmount())

	for routingKey, metrics := range keyMap {
		serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(metrics)

		err := m.AccumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, serializedMetricsBatch)
		if err != nil {
			return err
		}

		err = m.IndieReviewJoinExchange.Publish(routingKey, serializedMetricsBatch)
		if err != nil {
			return err
		}

	}

	return nil
}

func (m *Middleware) SendEof() error {
	indieReviewJoinerAmount := GetIndieReviewJoinersAmount()
	err := m.AccumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		return err
	}

	for nodeId := 1; nodeId <= indieReviewJoinerAmount; nodeId++ {
		err = m.IndieReviewJoinExchange.Publish(fmt.Sprintf("%s%d", IndieReviewJoinExchangeRoutingKeyPrefix, nodeId), sp.SerializeMsgEndOfFile())
		if err != nil {
			return err
		}
	}

	return nil
}

func GetIndieReviewJoinersAmount() int {
	indieReviewJoinersAmountString, err := u.GetEnv(IndieReviewJoinersAmountName)
	if err != nil {
		return 0
	}

	indieReviewJoinersAmount, err := strconv.Atoi(indieReviewJoinersAmountString)
	if err != nil {
		return 0
	}

	return indieReviewJoinersAmount
}

func idMapToKeyMap(idMap map[uint32]*r.GameReviewsMetrics, indieReviewJoinerAmount int) map[string][]*r.GameReviewsMetrics {
	keyMap := make(map[string][]*r.GameReviewsMetrics)
	for _, metrics := range idMap {
		key := u.GetPartitioningKeyFromInt(int(metrics.AppID), indieReviewJoinerAmount, IndieReviewJoinExchangeRoutingKeyPrefix)
		keyMap[key] = append(keyMap[key], metrics)
	}
	return keyMap
}
