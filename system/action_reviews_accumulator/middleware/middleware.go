package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	mom "distribuidos-tp/middleware"
	"fmt"
	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	ActionReviewsAccumulatorExchangeName     = "action_reviews_accumulator_exchange"
	ActionReviewsAccumulatorExchangeType     = "direct"
	ActionReviewsAccumulatorRoutingKeyPrefix = "action_reviews_accumulator_key_"
	ActionReviewsAccumulatorQueueNamePrefix  = "action_reviews_accumulator_queue_"

	AccumulatedReviewsExchangeName = "accumulated_reviews_exchange"
	AccumulatedReviewsExchangeType = "direct"
	AccumulatedReviewsRoutingKey   = "accumulated_reviews_key"
)

type Middleware struct {
	Manager                       *mom.MiddlewareManager
	ActionReviewsAccumulatorQueue *mom.Queue
	AccumulatedReviewsExchange    *mom.Exchange
	logger                        *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware manager: %v", err)
	}

	actionReviewsAccumulatorQueueName := fmt.Sprintf("%s%d", ActionReviewsAccumulatorQueueNamePrefix, id)
	actionReviewsAccumulatorRoutingKey := fmt.Sprintf("%s%d", ActionReviewsAccumulatorRoutingKeyPrefix, id)
	actionReviewsAccumulatorQueue, err := manager.CreateBoundQueue(actionReviewsAccumulatorQueueName, ActionReviewsAccumulatorExchangeName, ActionReviewsAccumulatorExchangeType, actionReviewsAccumulatorRoutingKey, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	accumulatedReviewsExchange, err := manager.CreateExchange(AccumulatedReviewsExchangeName, AccumulatedReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                       manager,
		ActionReviewsAccumulatorQueue: actionReviewsAccumulatorQueue,
		AccumulatedReviewsExchange:    accumulatedReviewsExchange,
		logger:                        logger,
	}, nil
}

func (m *Middleware) ReceiveReview(messageTracker *n.MessageTracker) (clientID int, reducedReview *r.ReducedReview, eof bool, newMessage bool, e error) {
	rawMsg, err := m.ActionReviewsAccumulatorQueue.Consume()
	if err != nil {
		return 0, nil, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return 0, nil, false, false, fmt.Errorf("failed to process message: %v", err)
	}

	if !newMessage {
		return message.ClientID, nil, false, false, nil
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, fmt.Errorf("failed to deserialize EOF: %v", err)
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, fmt.Errorf("failed to register EOF: %v", err)
		}

		return message.ClientID, nil, true, true, nil
	case sp.MsgReducedReviewInformation:
		review, err := sp.DeserializeMsgReducedReviewInformation(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, fmt.Errorf("failed to deserialize review: %v", err)
		}
		return message.ClientID, review, false, true, nil
	default:
		return message.ClientID, nil, false, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendAccumulatedReviews(clientID int, metrics []*ra.NamedGameReviewsMetrics) error {
	serializedMetricsBatch := sp.SerializeMsgNamedGameReviewsMetricsBatch(clientID, metrics)
	err := m.AccumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, serializedMetricsBatch)
	if err != nil {
		return fmt.Errorf("failed to publish accumulated reviews: %v", err)
	}
	m.logger.Infof("Sent message: %v", serializedMetricsBatch)
	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int) error {
	serializedEOF := sp.SerializeMsgEndOfFile(clientID)
	err := m.AccumulatedReviewsExchange.Publish(AccumulatedReviewsRoutingKey, serializedEOF)
	if err != nil {
		return fmt.Errorf("failed to publish end of file: %v", err)
	}
	m.logger.Infof("Sent EOF: %v", serializedEOF)
	return nil
}

func (m *Middleware) AckLastMessages() error {
	err := m.ActionReviewsAccumulatorQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
