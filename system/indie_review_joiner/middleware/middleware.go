package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	mom "distribuidos-tp/middleware"

	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	IndieReviewJoinExchangeName        = "indie_review_join_exchange"
	IndieReviewJoinExchangeType        = "direct"
	AccumulatedReviewsRoutingKeyPrefix = "accumulated_reviews_key_"
	IndieGameRoutingKeyPrefix          = "indie_key_"
	IndieReviewJoinQueueNamePrefix     = "indie_review_join_queue_"

	TopPositiveReviewsExchangeName = "top_positive_reviews_exchange"
	TopPositiveReviewsExchangeType = "direct"
	TopPositiveReviewsRoutingKey   = "top_positive_reviews_key"
)

type Middleware struct {
	Manager                 *mom.MiddlewareManager
	IndieReviewJoinQueue    *mom.Queue
	PositiveReviewsExchange *mom.Exchange
	logger                  *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	accumulatedReviewsRoutingKey := fmt.Sprintf("%s%d", AccumulatedReviewsRoutingKeyPrefix, id)
	indieGameRoutingKey := fmt.Sprintf("%s%d", IndieGameRoutingKeyPrefix, id)
	indieReviewJoinQueueName := fmt.Sprintf("%s%d", IndieReviewJoinQueueNamePrefix, id)

	routingKeys := []string{accumulatedReviewsRoutingKey, indieGameRoutingKey}
	indieReviewJoinQueue, err := manager.CreateBoundQueueMultipleRoutingKeys(indieReviewJoinQueueName, IndieReviewJoinExchangeName, IndieReviewJoinExchangeType, routingKeys, false)
	if err != nil {
		return nil, err
	}

	topPositiveReviewsExchange, err := manager.CreateExchange(TopPositiveReviewsExchangeName, TopPositiveReviewsExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                 manager,
		IndieReviewJoinQueue:    indieReviewJoinQueue,
		PositiveReviewsExchange: topPositiveReviewsExchange,
		logger:                  logger,
	}, nil
}

func (m *Middleware) ReceiveMsg(messageTracker *n.MessageTracker) (clientID int, games []*games.GameName, reviews []*reviews_accumulator.GameReviewsMetrics, eof bool, newMessage bool, e error) {
	rawMsg, err := m.IndieReviewJoinQueue.Consume()
	if err != nil {
		return 0, nil, nil, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, nil, false, false, err
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return message.ClientID, nil, nil, false, false, nil
	}

	if !newMessage {
		return message.ClientID, nil, nil, false, false, nil
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, false, fmt.Errorf("failed to deserialize EOF: %v", err)
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, nil, false, false, fmt.Errorf("failed to register EOF: %v", err)
		}
		return message.ClientID, nil, nil, true, true, nil
	case sp.MsgGameNames:
		gamesNames, err := HandleGameNames(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, false, err
		}
		return message.ClientID, gamesNames, nil, false, true, nil

	case sp.MsgGameReviewsMetrics:
		reviews, err := HandleGameReviewMetrics(message.Body)
		if err != nil {
			return message.ClientID, nil, nil, false, false, err
		}
		return message.ClientID, nil, reviews, false, true, err

	default:
		return message.ClientID, nil, nil, false, false, fmt.Errorf("unknown type msg")
	}

}

func HandleGameReviewMetrics(message []byte) ([]*reviews_accumulator.GameReviewsMetrics, error) {
	reviews, err := sp.DeserializeMsgGameReviewsMetricsBatch(message)
	if err != nil {
		return nil, err
	}
	return reviews, err
}

func HandleGameNames(message []byte) ([]*games.GameName, error) {
	gameNames, err := sp.DeserializeMsgGameNames(message)
	if err != nil {
		return nil, err
	}
	return gameNames, nil
}

func (m *Middleware) SendMetrics(clientID int, reviewsInformation *j.JoinedPositiveGameReview, messageTracker *n.MessageTracker) error {
	serializedMetrics, err := sp.SerializeMsgJoinedPositiveGameReviews(clientID, reviewsInformation)
	if err != nil {
		return err
	}

	err = m.PositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, serializedMetrics)
	messageTracker.RegisterSentMessage(clientID, TopPositiveReviewsRoutingKey)
	return err
}

func (m *Middleware) SendEof(clientID int, senderID int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)
	messagesSentToNode := messagesSent[TopPositiveReviewsRoutingKey]
	serializedMessage := sp.SerializeMsgEndOfFileV2(clientID, senderID, messagesSentToNode)
	err := m.PositiveReviewsExchange.Publish(TopPositiveReviewsRoutingKey, serializedMessage)
	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) AckLastMessage() error {
	err := m.IndieReviewJoinQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
