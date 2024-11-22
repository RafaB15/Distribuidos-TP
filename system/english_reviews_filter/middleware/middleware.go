package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawReviewsExchangeName           = "raw_english_reviews_exchange"
	RawReviewsExchangeType           = "direct"
	RawEnglishReviewsKeyPrefix       = "raw_english_reviews_key_"
	RawEnglishReviewsQueueNamePrefix = "raw_english_reviews_queue_"

	EnglishReviewsExchangeName     = "english_reviews_exchange"
	EnglishReviewsExchangeType     = "direct"
	EnglishReviewsRoutingKeyPrefix = "english_reviews_key_"
)

type Middleware struct {
	Manager                *mom.MiddlewareManager
	RawEnglishReviewsQueue *mom.Queue
	EnglishReviewsExchange *mom.Exchange
	logger                 *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware manager: %v", err)
	}

	rawEnglishReviewsQueueName := fmt.Sprintf("%s%d", RawEnglishReviewsQueueNamePrefix, id)
	rawEnglishReviewsRoutingKey := fmt.Sprintf("%s%d", RawEnglishReviewsKeyPrefix, id)

	rawEnglishReviewsQueue, err := manager.CreateBoundQueue(rawEnglishReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, rawEnglishReviewsRoutingKey, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	logger.Infof("Created queue %s bound to exchange %s and routing key %s\n", rawEnglishReviewsQueueName, RawReviewsExchangeName, rawEnglishReviewsRoutingKey)

	englishReviewsExchange, err := manager.CreateExchange(EnglishReviewsExchangeName, EnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                manager,
		RawEnglishReviewsQueue: rawEnglishReviewsQueue,
		EnglishReviewsExchange: englishReviewsExchange,
		logger:                 logger,
	}, nil
}

func (m *Middleware) ReceiveGameReviews(messageTracker *n.MessageTracker) (clientID int, rawReview *r.RawReview, eof bool, newMessage bool, e error) {
	rawMsg, err := m.RawEnglishReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, false, fmt.Errorf("failed to consume message: %v", err)
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
			return message.ClientID, nil, false, false, err
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, err
		}

		return message.ClientID, nil, true, true, nil
	case sp.MsgRawReviewInformation:
		rawReview, err := sp.DeserializeMsgRawReviewInformation(message.Body)
		if err != nil {
			return message.ClientID, nil, false, true, err
		}
		return message.ClientID, rawReview, false, true, nil
	default:
		return message.ClientID, nil, false, false, fmt.Errorf("unexpected message type: %d", message.Type)
	}
}

func (m *Middleware) SendEnglishReview(clientID int, review *r.Review, englishAccumulatorsAmount int) error {
	routingKey := u.GetPartitioningKeyFromInt(int(review.AppId), englishAccumulatorsAmount, EnglishReviewsRoutingKeyPrefix)
	serializedReview := sp.SerializeMsgReviewInformation(clientID, review)

	err := m.EnglishReviewsExchange.Publish(routingKey, serializedReview)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	m.logger.Infof("Sent review for client %d", clientID)

	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int, accumulatorsAmount int) error {
	for i := 1; i <= accumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, i)
		err := m.EnglishReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
	}

	return nil
}

func (m *Middleware) AckLastMessage() error {
	err := m.RawEnglishReviewsQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
