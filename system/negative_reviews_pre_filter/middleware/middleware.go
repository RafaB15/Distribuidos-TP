package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
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
	NegativePreFilterQueueMaxPriority = 1

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

	negativePreFilterQueue, err := manager.CreateBoundQueueWithPriority(negativePreFilterQueueName, NegativePreFilterExchangeName, NegativePreFilterExchangeType, negativePreFilterRoutingKey, false, NegativePreFilterQueueMaxPriority)
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

func (m *Middleware) ReceiveMessage(messageTracker *n.MessageTracker) (clientID int, rawReviews []*r.RawReview, reviewMetrics []*reviews_accumulator.GameReviewsMetrics, eof bool, newMessage bool, e error) {
	rawMsg, err := m.NegativePreFilterQueue.Consume()
	if err != nil {
		return 0, nil, nil, false, false, fmt.Errorf("failed to consume message: %v", err)
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, nil, false, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return 0, nil, nil, false, false, fmt.Errorf("failed to process message: %v", err)
	}

	if !newMessage {
		return message.ClientID, nil, nil, false, false, nil
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, nil, true, true, nil
	case sp.MsgRawReviewInformationBatch:
		rawReviews, err := sp.DeserializeMsgRawReviewInformationBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, false, err
		}
		return message.ClientID, rawReviews, nil, false, true, nil

	case sp.MsgGameReviewsMetrics:
		gameReviewsMetrics, err := sp.DeserializeMsgGameReviewsMetricsBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, false, err
		}
		return message.ClientID, nil, gameReviewsMetrics, false, true, nil
	default:
		return message.ClientID, nil, nil, false, false, fmt.Errorf("received unexpected message type: %v", message.Type)
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

func (m *Middleware) AckLastMessage() error {
	err := m.NegativePreFilterQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
