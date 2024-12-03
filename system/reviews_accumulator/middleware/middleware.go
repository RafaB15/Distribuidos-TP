package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	"distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"sort"

	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"
	ReviewQueueNamePrefix   = "reviews_queue_"

	IndieReviewJoinExchangeName             = "indie_review_join_exchange"
	IndieReviewJoinExchangeType             = "direct"
	IndieReviewJoinExchangeRoutingKeyPrefix = "accumulated_reviews_key_"
)

type Middleware struct {
	Manager                 *mom.MiddlewareManager
	ReviewsQueue            *mom.Queue
	IndieReviewJoinExchange *mom.Exchange
	logger                  *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	reviewQueueName := fmt.Sprintf("%s%v", ReviewQueueNamePrefix, id)
	reviewsRoutingKey := fmt.Sprintf("%s%v", ReviewsRoutingKeyPrefix, id)
	reviewsQueue, err := manager.CreateBoundQueue(reviewQueueName, ReviewsExchangeName, ReviewsExchangeType, reviewsRoutingKey, false)
	if err != nil {
		return nil, err
	}

	indieReviewJoinExchange, err := manager.CreateExchange(IndieReviewJoinExchangeName, IndieReviewJoinExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                 manager,
		ReviewsQueue:            reviewsQueue,
		IndieReviewJoinExchange: indieReviewJoinExchange,
		logger:                  logger,
	}, nil
}

func (m *Middleware) ReceiveReviews(messageTracker *n.MessageTracker) (clientID int, rawReviews []*reviews.RawReview, eof bool, newMessage bool, e error) {
	rawMsg, err := m.ReviewsQueue.Consume()
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
			return message.ClientID, nil, false, false, fmt.Errorf("failed to deserialize endOfFile: %v", err)
		}
		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, fmt.Errorf("failed to register EOF: %v", err)
		}
		return message.ClientID, nil, true, true, nil

	case sp.MsgRawReviewInformationBatch:
		reviewsInformation, err := sp.DeserializeMsgRawReviewInformationBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, fmt.Errorf("failed to deserialize reviewsInformation: %v", err)
		}
		return message.ClientID, reviewsInformation, false, true, nil
	default:
		return message.ClientID, nil, false, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendAccumulatedReviews(clientID int, accumulatedReviews *n.IntMap[*r.GameReviewsMetrics], indieReviewJoinersAmount int, messageTracker *n.MessageTracker) error {
	keyMap := idMapToKeyMap(accumulatedReviews, indieReviewJoinersAmount, IndieReviewJoinExchangeRoutingKeyPrefix)

	for routingKey, metrics := range keyMap {

		sort.Slice(metrics, func(i, j int) bool {
			return metrics[i].AppID < metrics[j].AppID
		})

		serializedMetricsBatch := sp.SerializeMsgGameReviewsMetricsBatch(clientID, metrics)

		err := m.IndieReviewJoinExchange.Publish(routingKey, serializedMetricsBatch)
		if err != nil {
			return err
		}
		messageTracker.RegisterSentMessage(clientID, routingKey)
	}

	return nil
}

func (m *Middleware) SendEof(clientID int, senderID int, indieReviewJoinersAmount int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)

	for nodeId := 1; nodeId <= indieReviewJoinersAmount; nodeId++ {
		routingKey := fmt.Sprintf("%s%d", IndieReviewJoinExchangeRoutingKeyPrefix, nodeId)
		messagesSentToNode := messagesSent[routingKey]
		serializedMsg := sp.SerializeMsgEndOfFile(clientID, senderID, messagesSentToNode)
		err := m.IndieReviewJoinExchange.Publish(routingKey, serializedMsg)
		if err != nil {
			return err
		}
	}

	return nil
}

func idMapToKeyMap(idMap *n.IntMap[*r.GameReviewsMetrics], nodeAmount int, routingKeyPrefix string) map[string][]*r.GameReviewsMetrics {
	keyMap := make(map[string][]*r.GameReviewsMetrics)
	idMap.Range(func(key int, value *r.GameReviewsMetrics) bool {
		partitionKey := u.GetPartitioningKeyFromInt(key, nodeAmount, routingKeyPrefix)
		keyMap[partitionKey] = append(keyMap[partitionKey], value)
		return true
	})
	return keyMap
}

func (m *Middleware) AckLastMessage() error {
	err := m.ReviewsQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
