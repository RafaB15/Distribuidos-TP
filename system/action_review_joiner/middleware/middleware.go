package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	ActionReviewJoinerExchangeName     = "action_review_joiner_exchange"
	ActionReviewJoinerExchangeType     = "direct"
	ActionReviewJoinerRoutingKeyPrefix = "action_review_joiner_key_"
	ActionReviewJoinerQueueNamePrefix  = "action_review_joiner_queue_"
	ActionReviewJoinerQueueMaxPriority = 1

	RawEnglishReviewsExchangeName     = "raw_english_reviews_exchange"
	RawEnglishReviewsExchangeType     = "direct"
	RawEnglishReviewsRoutingKeyPrefix = "raw_english_reviews_key_"
)

type Middleware struct {
	Manager                   *mom.MiddlewareManager
	ActionReviewJoinerQueue   *mom.Queue
	RawEnglishReviewsExchange *mom.Exchange
	logger                    *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	actionReviewJoinerQueueName := fmt.Sprintf("%s%d", ActionReviewJoinerQueueNamePrefix, id)
	actionReviewJoinerRoutingKey := fmt.Sprintf("%s%d", ActionReviewJoinerRoutingKeyPrefix, id)

	actionReviewJoinerQueue, err := manager.CreateBoundQueueWithPriority(actionReviewJoinerQueueName, ActionReviewJoinerExchangeName, ActionReviewJoinerExchangeType, actionReviewJoinerRoutingKey, false, ActionReviewJoinerQueueMaxPriority)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	rawEnglishReviewsExchange, err := manager.CreateExchange(RawEnglishReviewsExchangeName, RawEnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                   manager,
		ActionReviewJoinerQueue:   actionReviewJoinerQueue,
		RawEnglishReviewsExchange: rawEnglishReviewsExchange,
		logger:                    logger,
	}, nil
}

func (m *Middleware) ReceiveMessage(messageTracker *n.MessageTracker) (clientID int, rawReviews []*r.RawReview, reviewMetrics []*reviews_accumulator.GameReviewsMetrics, eof bool, newMessage bool, e error) {
	rawMsg, err := m.ActionReviewJoinerQueue.Consume()
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
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, false, err
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, nil, false, false, err
		}

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

func (m *Middleware) SendRawReview(clientID int, englishFiltersAmount int, rawReview *r.RawReview, messageTracker *n.MessageTracker) error {
	routingKey := u.GetPartitioningKeyFromInt(int(rawReview.ReviewId), englishFiltersAmount, RawEnglishReviewsRoutingKeyPrefix)
	serializedMsg := sp.SerializeMsgRawReviewInformation(clientID, rawReview)
	err := m.RawEnglishReviewsExchange.Publish(routingKey, serializedMsg)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	messageTracker.RegisterSentMessage(clientID, routingKey)

	return nil
}

func (m *Middleware) SendEndOfFile(clientID int, senderID int, englishFiltersAmount int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)

	for i := 1; i <= englishFiltersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", RawEnglishReviewsRoutingKeyPrefix, i)
		messagesSentToNode := messagesSent[routingKey]
		serializedMsg := sp.SerializeMsgEndOfFileV2(clientID, senderID, messagesSentToNode)
		err := m.RawEnglishReviewsExchange.Publish(routingKey, serializedMsg)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
		m.logger.Infof("Sent EOF to english filter %d with routing key %s", i, routingKey)
	}

	return nil
}

func (m *Middleware) AckLastMessage() error {
	err := m.ActionReviewJoinerQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
