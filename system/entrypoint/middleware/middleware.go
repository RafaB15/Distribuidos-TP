package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
	"github.com/op/go-logging"
)

const (
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"

	ActionReviewJoinerExchangeName     = "action_review_joiner_exchange"
	ActionReviewJoinerExchangeType     = "direct"
	ActionReviewJoinerRoutingKeyPrefix = "action_review_joiner_key_"

	QueryResultsQueueNamePrefix = "query_results_queue_"
	QueryResultsExchangeName    = "query_results_exchange"
	QueryRoutingKeyPrefix       = "query_results_key_" // con el id del cliente
	QueryExchangeType           = "direct"

	AppIdIndex       = 0
	ReviewTextIndex  = 1
	ReviewScoreIndex = 2
)

type Middleware struct {
	Manager                    *mom.MiddlewareManager
	RawGamesExchange           *mom.Exchange
	ReviewsExchange            *mom.Exchange
	ActionReviewJoinerExchange *mom.Exchange
	QueryResultsQueue          *mom.Queue
	logger                     *logging.Logger
}

func NewMiddleware(clientID int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware manager: %v", err)
	}

	rawGamesExchange, err := manager.CreateExchange(RawGamesExchangeName, RawGamesExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	reviewsExchange, err := manager.CreateExchange(ReviewsExchangeName, ReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	actionReviewJoinerExchange, err := manager.CreateExchange(ActionReviewJoinerExchangeName, ActionReviewJoinerExchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	queryResultsQueueName := fmt.Sprintf("%s%d", QueryResultsQueueNamePrefix, clientID)
	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	queryResultsQueue, err := manager.CreateBoundQueue(queryResultsQueueName, QueryResultsExchangeName, QueryExchangeType, routingKey, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %v", err)
	}

	return &Middleware{
		Manager:                    manager,
		RawGamesExchange:           rawGamesExchange,
		ReviewsExchange:            reviewsExchange,
		ActionReviewJoinerExchange: actionReviewJoinerExchange,
		QueryResultsQueue:          queryResultsQueue,
		logger:                     logger,
	}, nil
}

func (m *Middleware) SendGamesBatch(clientID int, data []byte, messageTracker *n.MessageTracker) error {
	batch := sp.SerializeMsgBatch(clientID, data)
	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, batch)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	messageTracker.RegisterSentMessage(clientID, RawGamesRoutingKey)
	return nil
}

func (m *Middleware) SendReviewsBatch(clientID int, actionReviewJoinersAmount int, reviewAccumulatorsAmount int, data []byte, currentReviewId int, messageTracker *n.MessageTracker) (sentReviewsAmount int, e error) {
	rawReviews, err := getDeserializedRawReviews(data, currentReviewId)
	if err != nil {
		return 0, fmt.Errorf("failed to deserialize raw reviews: %v", err)
	}

	err = sendToReviewNode(clientID, actionReviewJoinersAmount, m.ActionReviewJoinerExchange, ActionReviewJoinerRoutingKeyPrefix, rawReviews, messageTracker, m.logger)
	if err != nil {
		return 0, fmt.Errorf("failed to publish message to negative pre filter: %v", err)
	}

	err = sendToReviewNode(clientID, reviewAccumulatorsAmount, m.ReviewsExchange, ReviewsRoutingKeyPrefix, rawReviews, messageTracker, m.logger)
	if err != nil {
		return 0, fmt.Errorf("failed to publish message to review mapper: %v", err)
	}

	return len(rawReviews), nil
}

func sendToReviewNode(clientID int, nodesAmount int, exchange *mom.Exchange, routingKeyPrefix string, rawReviews []*r.RawReview, messageTracker *n.MessageTracker, logger *logging.Logger) error {
	routingKeyMap := make(map[string][]*r.RawReview)
	for _, rawReview := range rawReviews {
		routingKey := u.GetPartitioningKeyFromInt(int(rawReview.AppId), nodesAmount, routingKeyPrefix)
		routingKeyMap[routingKey] = append(routingKeyMap[routingKey], rawReview)
	}

	for routingKey, reviews := range routingKeyMap {
		serializedReviews := sp.SerializeMsgRawReviewInformationBatch(clientID, reviews)
		err := exchange.Publish(routingKey, serializedReviews)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
		messageTracker.RegisterSentMessage(clientID, routingKey)
	}

	return nil
}

func getDeserializedRawReviews(data []byte, currentReviewId int) ([]*r.RawReview, error) {
	lines, err := sp.DeserializeMsgBatch(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}

	rawReviews, err := r.DeserializeRawReviewsBatchFromStrings(lines, AppIdIndex, ReviewScoreIndex, ReviewTextIndex, currentReviewId)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize raw reviews: %v", err)
	}

	return rawReviews, nil
}

func (m *Middleware) SendGamesEndOfFile(clientID int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)
	messagesSentToGameMapper := messagesSent[RawGamesRoutingKey]
	serializedMessage := sp.SerializeMsgEndOfFile(clientID, 0, messagesSentToGameMapper)

	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, serializedMessage)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendReviewsEndOfFile(clientID int, actionReviewJoinersAmount int, reviewAccumulatorsAmount int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)

	for i := 1; i <= actionReviewJoinersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ActionReviewJoinerRoutingKeyPrefix, i)
		serializedMessage := sp.SerializeMsgEndOfFile(clientID, 0, messagesSent[routingKey])
		err := m.ActionReviewJoinerExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
		fmt.Printf("Sent EOF to negative pre filter %d\n", i)
	}

	for i := 1; i <= reviewAccumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ReviewsRoutingKeyPrefix, i)
		serializedMessage := sp.SerializeMsgEndOfFile(clientID, 0, messagesSent[routingKey])
		err := m.ReviewsExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
	}

	return nil
}

func (m *Middleware) ReceiveQueryResponse(querysArrived map[int]bool) ([]byte, bool, error) {
	rawMsg, err := m.QueryResultsQueue.Consume()
	if err != nil {
		return nil, false, fmt.Errorf("failed to consume message: %v", err)
	}

	queryResponseMessage, err := sp.DeserializeQuery(rawMsg)

	if querysArrived[int(queryResponseMessage.Type)] {
		return nil, true, nil
	}

	querysArrived[int(queryResponseMessage.Type)] = true

	if err != nil {
		return nil, false, fmt.Errorf("failed to deserialize message: %v", err)
	}
	// fmt.Printf("Received query response of type: %d\n", queryResponseMessage.Type)
	switch queryResponseMessage.Type {
	case sp.MsgOsResolvedQuery:
		fmt.Printf("Received OS resolved query\n")
		result, err := handleMsgOsResolvedQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)
		return result, false, err

	case sp.MsgTopTenDecadeAvgPtfQuery:
		fmt.Printf("Received decade reviews query\n")
		result, err := handleMsgTopTenResolvedQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)
		return result, false, err
	case sp.MsgIndiePositiveJoinedReviewsQuery:
		fmt.Printf("Received positive indie reviews query\n")
		result, err := handleMsgIndiePositiveResolvedQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)
		return result, false, err
	case sp.MsgActionNegativeEnglishReviewsQuery:
		fmt.Printf("Received positive reviews query\n")
		result, err := handleMsgActionNegativeEnglishReviewsQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)
		return result, false, err
	case sp.MsgActionNegativeReviewsQuery:
		fmt.Printf("Received negative reviews query\n")
		result, err := handleMsgActionNegativeReviewsQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)
		return result, false, err
	default:
		fmt.Printf("Received unknown query response\n")
	}
	return rawMsg, false, nil
}

func handleMsgOsResolvedQuery(clientID int, message []byte) ([]byte, error) {
	gameOSMetrics, err := sp.DeserializeMsgOsResolvedQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}
	stringRepresentation := oa.GetStrRepresentation(gameOSMetrics)

	// fmt.Println("String Representation:", stringRepresentation)
	return sp.AssembleFinalQueryMsg(byte(clientID), byte(sp.MsgOsResolvedQuery), []byte(stringRepresentation)), nil
}

func handleMsgIndiePositiveResolvedQuery(clientID int, message []byte) ([]byte, error) {
	joinedReviews, err := sp.DeserializeMsgIndiePositiveJoinedReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}
	var stringRepresentation []byte
	for _, review := range joinedReviews {
		stringRep := j.GetStrRepresentation(review)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(clientID), byte(sp.MsgIndiePositiveJoinedReviewsQuery), stringRepresentation), nil
}

func handleMsgActionNegativeEnglishReviewsQuery(clientID int, message []byte) ([]byte, error) {
	namedGamesReviewsMetrics, err := sp.DeserializeMsgActionNegativeEnglishReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}

	var stringRepresentation []byte
	for _, reviewMetrics := range namedGamesReviewsMetrics {
		stringRep := ra.GetStrRepresentationGameReviewsMetricsOnlyName(reviewMetrics)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(clientID), byte(sp.MsgActionNegativeEnglishReviewsQuery), stringRepresentation), nil
}

func handleMsgActionNegativeReviewsQuery(clientID int, message []byte) ([]byte, error) {
	gameMetricsAbovePercentile, err := sp.DeserializeMsgActionNegativeReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}

	var stringRepresentation []byte
	for _, review := range gameMetricsAbovePercentile {
		stringRep := ra.GetStrRepresentationGameReviewsMetrics(review)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(clientID), byte(sp.MsgActionNegativeReviewsQuery), stringRepresentation), nil
}

func handleMsgTopTenResolvedQuery(clientID int, message []byte) ([]byte, error) {
	decadeAvgPtfs, err := sp.DeserializeMsgTopTenResolvedQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}
	stringRepresentation := ""
	for _, decadeAvgPtf := range decadeAvgPtfs {
		stringRepresentation += df.GetStrRepresentation(decadeAvgPtf)

	}
	// fmt.Println("String Representation:", stringRepresentation)

	return sp.AssembleFinalQueryMsg(byte(clientID), byte(sp.MsgTopTenDecadeAvgPtfQuery), []byte(stringRepresentation)), nil

}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
