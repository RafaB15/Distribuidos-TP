package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	mom "distribuidos-tp/middleware"
	"fmt"
	"github.com/op/go-logging"
	"sort"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	EnglishReviewsExchangeName     = "english_reviews_exchange"
	EnglishReviewsExchangeType     = "direct"
	EnglishReviewsRoutingKeyPrefix = "english_reviews_key_"
	EnglishReviewQueueNamePrefix   = "english_reviews_queue_"

	AccumulatedEnglishReviewsExchangeName = "accumulated_english_reviews_exchange"
	AccumulatedEnglishReviewsExchangeType = "direct"
	AccumulatedEnglishReviewsRoutingKey   = "accumulated_english_reviews_key"
)

type Middleware struct {
	Manager                           *mom.MiddlewareManager
	EnglishReviewsQueue               *mom.Queue
	AccumulatedEnglishReviewsExchange *mom.Exchange
	logger                            *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware manager: %v", err)
	}

	englishReviewQueueName := fmt.Sprintf("%s%d", EnglishReviewQueueNamePrefix, id)
	englishReviewsRoutingKey := fmt.Sprintf("%s%d", EnglishReviewsRoutingKeyPrefix, id)
	englishReviewsQueue, err := manager.CreateBoundQueue(englishReviewQueueName, EnglishReviewsExchangeName, EnglishReviewsExchangeType, englishReviewsRoutingKey, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	accumulatedEnglishReviewsExchange, err := manager.CreateExchange(AccumulatedEnglishReviewsExchangeName, AccumulatedEnglishReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                           manager,
		EnglishReviewsQueue:               englishReviewsQueue,
		AccumulatedEnglishReviewsExchange: accumulatedEnglishReviewsExchange,
		logger:                            logger,
	}, nil
}

func (m *Middleware) ReceiveReview(messageTracker *n.MessageTracker) (clientID int, reducedReview *r.ReducedReview, eof bool, newMessage bool, delMessage bool, e error) {
	rawMsg, err := m.EnglishReviewsQueue.Consume()
	if err != nil {
		return 0, nil, false, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, false, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return 0, nil, false, false, false, fmt.Errorf("failed to process message: %v", err)
	}

	if !newMessage {
		return message.ClientID, nil, false, false, false, nil
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, fmt.Errorf("failed to deserialize EOF: %v", err)
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, false, fmt.Errorf("failed to register EOF: %v", err)
		}

		return message.ClientID, nil, true, true, false, nil
	case sp.MsgDeleteClient:
		m.logger.Infof("Received delete client message from client %d", message.ClientID)
		return message.ClientID, nil, false, true, true, nil
	case sp.MsgReducedReviewInformation:
		review, err := sp.DeserializeMsgReducedReviewInformation(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, fmt.Errorf("failed to deserialize reducedReviews: %v", err)
		}
		return message.ClientID, review, false, true, false, nil
	default:
		return message.ClientID, nil, false, false, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendAccumulatedReviews(clientID int, metrics []*ra.NamedGameReviewsMetrics, messageTracker *n.MessageTracker) error {
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].AppID < metrics[j].AppID
	})

	serializedMetricsBatch := sp.SerializeMsgNamedGameReviewsMetricsBatch(clientID, metrics)
	err := m.AccumulatedEnglishReviewsExchange.Publish(AccumulatedEnglishReviewsRoutingKey, serializedMetricsBatch)
	if err != nil {
		return fmt.Errorf("failed to publish accumulated reviews: %v", err)
	}

	messageTracker.RegisterSentMessage(clientID, AccumulatedEnglishReviewsRoutingKey)
	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int, senderID int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)
	messagesSentToNode := messagesSent[AccumulatedEnglishReviewsRoutingKey]
	serializedEOF := sp.SerializeMsgEndOfFile(clientID, senderID, messagesSentToNode)
	err := m.AccumulatedEnglishReviewsExchange.Publish(AccumulatedEnglishReviewsRoutingKey, serializedEOF)
	if err != nil {
		return fmt.Errorf("failed to publish end of file: %v", err)
	}
	return nil
}

func (m *Middleware) SendDeleteClient(clientID int) error {
	serializedDeleteClient := sp.SerializeMsgDeleteClient(clientID)
	err := m.AccumulatedEnglishReviewsExchange.Publish(AccumulatedEnglishReviewsRoutingKey, serializedDeleteClient)
	if err != nil {
		return fmt.Errorf("failed to publish delete client: %v", err)
	}
	return nil
}

func (m *Middleware) AckLastMessages() error {
	err := m.EnglishReviewsQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
