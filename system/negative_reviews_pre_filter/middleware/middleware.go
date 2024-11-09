package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	NegativePreFilterExchangeName     = "negative_pre_filter_exchange"
	NegativePreFilterExchangeType     = "direct"
	NegativePreFilterRoutingKeyPrefix = "negative_pre_filter_key_"
	NegativePreFilterQueueNamePrefix  = "negative_pre_filter_queue_"

	RawEnglishReviewsExchangeName     = "raw_english_reviews_exchange"
	RawEnglishReviewsExchangeType     = "direct"
	RawEnglishReviewsRoutingKeyPrefix = "raw_english_reviews_key_"
)

type Middleware struct {
	Manager                   *mom.MiddlewareManager
	NegativePreFilterQueue    *mom.Queue
	RawEnglishReviewsExchange *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	negativePreFilterQueueName := fmt.Sprintf("%s%d", NegativePreFilterQueueNamePrefix, id)
	negativePreFilterRoutingKey := fmt.Sprintf("%s%d", NegativePreFilterRoutingKeyPrefix, id)

	negativePreFilterQueue, err := manager.CreateBoundQueue(negativePreFilterQueueName, NegativePreFilterExchangeName, NegativePreFilterExchangeType, negativePreFilterRoutingKey, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	rawEnglishReviewsExchange, err := manager.CreateExchange(RawEnglishReviewsExchangeName, RawEnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                   manager,
		NegativePreFilterQueue:    negativePreFilterQueue,
		RawEnglishReviewsExchange: rawEnglishReviewsExchange,
	}, nil
}

func (m *Middleware) ReceiveMessage() (int, *r.RawReview, []*reviews_accumulator.GameReviewsMetrics, bool, error) {
	rawMsg, err := m.NegativePreFilterQueue.Consume()
	if err != nil {
		return 0, nil, nil, false, fmt.Errorf("failed to consume message: %v", err)
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, nil, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, nil, true, nil
	case sp.MsgRawReviewInformation:
		rawReview, err := sp.DeserializeMsgRawReviewInformation(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, err
		}
		return message.ClientID, rawReview, nil, false, nil

	case sp.MsgGameReviewsMetrics:
		gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, err
		}
		return message.ClientID, nil, gameReviewsMetrics, false, nil
	default:
		return message.ClientID, nil, nil, false, fmt.Errorf("received unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendRawReview(clientID int, englishFiltersAmount int, rawReview *r.RawReview) error {
	nodeId := u.GetRandomNumber(englishFiltersAmount)
	routingKey := fmt.Sprintf("%s%d", RawEnglishReviewsRoutingKeyPrefix, nodeId)
	serializedMsg := sp.SerializeMsgRawReviewInformation(clientID, rawReview)
	err := m.RawEnglishReviewsExchange.Publish(routingKey, serializedMsg)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendEndOfFile(clientID int, englishFiltersAmount int) error {
	for i := 1; i <= englishFiltersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", RawEnglishReviewsRoutingKeyPrefix, i)
		serializedMsg := sp.SerializeMsgEndOfFile(clientID)
		err := m.RawEnglishReviewsExchange.Publish(routingKey, serializedMsg)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
		fmt.Printf("Sent EOF to english filter %d with routing key %s\n", i, routingKey)
	}

	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
