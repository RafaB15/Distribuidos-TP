package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
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

	NegativePreFilterExchangeName     = "negative_pre_filter_exchange"
	NegativePreFilterExchangeType     = "direct"
	NegativePreFilterRoutingKeyPrefix = "negative_pre_filter_key_"

	IndieReviewJoinExchangeName             = "indie_review_join_exchange"
	IndieReviewJoinExchangeType             = "direct"
	IndieReviewJoinExchangeRoutingKeyPrefix = "accumulated_reviews_key_"
)

type Middleware struct {
	Manager                    *mom.MiddlewareManager
	ReviewsQueue               *mom.Queue
	AccumulatedReviewsExchange *mom.Exchange
	NegativeReviewsPreFilter   *mom.Exchange
	IndieReviewJoinExchange    *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	reviewQueueName := fmt.Sprintf("%s%v", ReviewQueueNamePrefix, id)
	reviewsRoutingKey := fmt.Sprintf("%s%v", ReviewsRoutingKeyPrefix, id)
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

	negativeReviewsPreFilterExchange, err := manager.CreateExchange(NegativePreFilterExchangeName, NegativePreFilterExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                    manager,
		ReviewsQueue:               reviewsQueue,
		AccumulatedReviewsExchange: accumulatedReviewsExchange,
		NegativeReviewsPreFilter:   negativeReviewsPreFilterExchange,
		IndieReviewJoinExchange:    indieReviewJoinExchange,
	}, nil
}

func (m *Middleware) ReceiveReviews() (int, []*reviews.Review, bool, error) {
	rawMsg, err := m.ReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, fmt.Errorf("Failed to deserialize message: %v", err)
	}

	fmt.Printf("Received message from client %d\n", message.ClientID)

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	case sp.MsgReviewInformation:
		reviewsInformation, err := sp.DeserializeMsgReviewInformation(message.Body)
		if err != nil {
			return message.ClientID, nil, false, fmt.Errorf("failed to deserialize reviewsInformation: %v", err)
		}
		return message.ClientID, reviewsInformation, false, nil
	default:
		return message.ClientID, nil, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendAccumulatedReviews(clientID int, accumulatedReviews map[uint32]*r.GameReviewsMetrics, indieReviewJoinersAmount int, negativeReviewPreFiltersAmount int) error {
	keyMap := idMapToKeyMap(accumulatedReviews, indieReviewJoinersAmount, IndieReviewJoinExchangeRoutingKeyPrefix)

	for routingKey, metrics := range keyMap {
		serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(clientID, metrics)

		err := m.AccumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, serializedMetricsBatch)
		if err != nil {
			return err
		}

		err = m.IndieReviewJoinExchange.Publish(routingKey, serializedMetricsBatch)
		if err != nil {
			return err
		}
	}

	preFilterKeyMap := idMapToKeyMap(accumulatedReviews, negativeReviewPreFiltersAmount, NegativePreFilterRoutingKeyPrefix)
	for routingKey, metrics := range preFilterKeyMap {
		serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(clientID, metrics)

		err := m.NegativeReviewsPreFilter.Publish(routingKey, serializedMetricsBatch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Middleware) SendEof(clientID int, indieReviewJoinersAmount int, negativeReviewPreFiltersAmount int) error {
	err := m.AccumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, sp.SerializeMsgEndOfFile(clientID))
	if err != nil {
		return err
	}

	for nodeId := 1; nodeId <= indieReviewJoinersAmount; nodeId++ {
		err = m.IndieReviewJoinExchange.Publish(fmt.Sprintf("%s%d", IndieReviewJoinExchangeRoutingKeyPrefix, nodeId), sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return err
		}
	}

	for nodeId := 1; nodeId <= negativeReviewPreFiltersAmount; nodeId++ {
		err = m.NegativeReviewsPreFilter.Publish(fmt.Sprintf("%s%d", NegativePreFilterRoutingKeyPrefix, nodeId), sp.SerializeMsgEndOfFile(clientID))
		fmt.Printf("Sending EOF to negative pre filter %d\n", nodeId)
		if err != nil {
			return err
		}
	}

	return nil
}

func idMapToKeyMap(idMap map[uint32]*r.GameReviewsMetrics, nodeAmount int, routingKeyPrefix string) map[string][]*r.GameReviewsMetrics {
	keyMap := make(map[string][]*r.GameReviewsMetrics)
	for _, metrics := range idMap {
		key := u.GetPartitioningKeyFromInt(int(metrics.AppID), nodeAmount, routingKeyPrefix)
		keyMap[key] = append(keyMap[key], metrics)
	}
	return keyMap
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
