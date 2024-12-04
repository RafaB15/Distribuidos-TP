package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	g "distribuidos-tp/internal/system_protocol/games"
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

	ActionReviewsAccumulatorExchangeName     = "action_reviews_accumulator_exchange"
	ActionReviewsAccumulatorExchangeType     = "direct"
	ActionReviewsAccumulatorRoutingKeyPrefix = "action_reviews_accumulator_key_"
)

type Middleware struct {
	Manager                          *mom.MiddlewareManager
	ActionReviewJoinerQueue          *mom.Queue
	RawEnglishReviewsExchange        *mom.Exchange
	ActionReviewsAccumulatorExchange *mom.Exchange
	logger                           *logging.Logger
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

	actionReviewsAccumulatorExchange, err := manager.CreateExchange(ActionReviewsAccumulatorExchangeName, ActionReviewsAccumulatorExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	return &Middleware{
		Manager:                          manager,
		ActionReviewJoinerQueue:          actionReviewJoinerQueue,
		RawEnglishReviewsExchange:        rawEnglishReviewsExchange,
		ActionReviewsAccumulatorExchange: actionReviewsAccumulatorExchange,
		logger:                           logger,
	}, nil
}

func (m *Middleware) ReceiveMessage(messageTracker *n.MessageTracker) (clientID int, reviews []*r.RawReview, games []*g.Game, eof bool, newMessage bool, e error) {
	rawMsg, err := m.ActionReviewJoinerQueue.Consume()
	if err != nil {
		e = fmt.Errorf("failed to consume message: %v", err)
		return
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		e = fmt.Errorf("failed to deserialize message: %v", err)
		return
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		e = fmt.Errorf("failed to process message: %v", err)
		return
	}

	if !newMessage {
		clientID = message.ClientID
		return
	}

	clientID = message.ClientID
	newMessage = true

	switch message.Type {
	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			e = err
			return
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			e = err
			return
		}
		eof = true
	case sp.MsgRawReviewInformationBatch:
		reviews, err = sp.DeserializeMsgRawReviewInformationBatch(message.Body)
		if err != nil {
			e = err
			return
		}
	case sp.MsgGames:
		games, err = sp.DeserializeMsgGames(message.Body)
		if err != nil {
			e = err
			return
		}
	default:
		e = fmt.Errorf("received unexpected message type: %v", message.Type)
	}

	return
}

func (m *Middleware) SendReview(clientID int, englishFiltersAmount int, actionReviewsAccumulatorsAmount int, review *r.Review, messageTracker *n.MessageTracker) error {
	englishRoutingKey := u.GetPartitioningKeyFromInt(int(review.ReviewId), englishFiltersAmount, RawEnglishReviewsRoutingKeyPrefix)
	serializedMsg := sp.SerializeMsgReviewInformation(clientID, review)
	err := m.RawEnglishReviewsExchange.Publish(englishRoutingKey, serializedMsg)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	messageTracker.RegisterSentMessage(clientID, englishRoutingKey)

	actionAccumulatorRoutingKey := u.GetPartitioningKeyFromInt(int(review.AppId), actionReviewsAccumulatorsAmount, ActionReviewsAccumulatorRoutingKeyPrefix)
	reducedReview := r.NewReducedReview(review.ReviewId, review.AppId, review.Name, review.Positive)
	serializedMsg = sp.SerializeMsgReducedReviewInformation(clientID, reducedReview)
	err = m.ActionReviewsAccumulatorExchange.Publish(actionAccumulatorRoutingKey, serializedMsg)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	messageTracker.RegisterSentMessage(clientID, actionAccumulatorRoutingKey)

	return nil
}

func (m *Middleware) SendEndOfFile(clientID int, senderID int, englishFiltersAmount int, actionReviewsAccumulatorsAmount int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)

	for i := 1; i <= englishFiltersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", RawEnglishReviewsRoutingKeyPrefix, i)
		messagesSentToNode := messagesSent[routingKey]
		serializedMsg := sp.SerializeMsgEndOfFile(clientID, senderID, messagesSentToNode)
		err := m.RawEnglishReviewsExchange.Publish(routingKey, serializedMsg)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
		m.logger.Infof("Sent EOF to english filter %d with routing key %s", i, routingKey)
	}

	for i := 1; i <= actionReviewsAccumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ActionReviewsAccumulatorRoutingKeyPrefix, i)
		messagesSentToNode := messagesSent[routingKey]
		serializedMsg := sp.SerializeMsgEndOfFile(clientID, senderID, messagesSentToNode)
		err := m.ActionReviewsAccumulatorExchange.Publish(routingKey, serializedMsg)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
		m.logger.Infof("Sent EOF to action reviews accumulator %d with routing key %s", i, routingKey)
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
