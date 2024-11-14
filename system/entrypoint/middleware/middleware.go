package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	j "distribuidos-tp/internal/system_protocol/joiner"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"

	NegativePreFilterExchangeName     = "negative_pre_filter_exchange"
	NegativePreFilterExchangeType     = "direct"
	NegativePreFilterRoutingKeyPrefix = "negative_pre_filter_key_"

	QueryResultsQueueNamePrefix = "query_results_queue_"
	QueryResultsExchangeName    = "query_results_exchange"
	QueryRoutingKeyPrefix       = "query_results_key_" // con el id del cliente
	QueryExchangeType           = "direct"

	AppIdIndex       = 0
	ReviewTextIndex  = 1
	ReviewScoreIndex = 2
)

type Middleware struct {
	Manager                   *mom.MiddlewareManager
	RawGamesExchange          *mom.Exchange
	ReviewsExchange           *mom.Exchange
	NegativePreFilterExchange *mom.Exchange
	QueryResultsQueue         *mom.Queue
}

func NewMiddleware(clientID int) (*Middleware, error) {
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

	negativePreFilterExchange, err := manager.CreateExchange(NegativePreFilterExchangeName, NegativePreFilterExchangeType)
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
		Manager:                   manager,
		RawGamesExchange:          rawGamesExchange,
		ReviewsExchange:           reviewsExchange,
		NegativePreFilterExchange: negativePreFilterExchange,
		QueryResultsQueue:         queryResultsQueue,
	}, nil
}

func (m *Middleware) SendGamesBatch(clientID int, data []byte) error {
	batch := sp.SerializeMsgBatch(clientID, data)
	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, batch)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendReviewsBatch(clientID int, negativeReviewsPreFiltersAmount int, reviewAccumulatorsAmount int, data []byte) error {
	rawReviews, err := getDeserializedRawReviews(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize raw reviews: %v", err)
	}

	err = sendToReviewNode(clientID, negativeReviewsPreFiltersAmount, m.NegativePreFilterExchange, NegativePreFilterRoutingKeyPrefix, rawReviews)
	if err != nil {
		return fmt.Errorf("failed to publish message to negative pre filter: %v", err)
	}

	err = sendToReviewNode(clientID, reviewAccumulatorsAmount, m.ReviewsExchange, ReviewsRoutingKeyPrefix, rawReviews)
	if err != nil {
		return fmt.Errorf("failed to publish message to review mapper: %v", err)
	}

	return nil
}

func sendToReviewNode(clientID int, nodesAmount int, exchange *mom.Exchange, routingKeyPrefix string, rawReviews []*r.RawReview) error {
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
	}

	return nil
}

func (m *Middleware) sendToPreFilterNode(clientID int, reviewsPreFilterNodesAmount int, data []byte) error {
	lines, err := sp.DeserializeMsgBatch(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %v", err)
	}

	rawReviews, err := r.DeserializeRawReviewsBatchFromStrings(lines, AppIdIndex, ReviewScoreIndex, ReviewTextIndex)
	if err != nil {
		return fmt.Errorf("failed to deserialize raw reviews: %v", err)
	}

	for _, rawReview := range rawReviews {
		shardingKey := u.GetPartitioningKeyFromInt(int(rawReview.AppId), reviewsPreFilterNodesAmount, NegativePreFilterRoutingKeyPrefix)
		serializedMsg := sp.SerializeMsgRawReviewInformation(clientID, rawReview)
		err := m.NegativePreFilterExchange.Publish(shardingKey, serializedMsg)
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
	}

	return nil
}

func getDeserializedRawReviews(data []byte) ([]*r.RawReview, error) {
	lines, err := sp.DeserializeMsgBatch(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}

	rawReviews, err := r.DeserializeRawReviewsBatchFromStrings(lines, AppIdIndex, ReviewScoreIndex, ReviewTextIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize raw reviews: %v", err)
	}

	return rawReviews, nil
}

func (m *Middleware) SendGamesEndOfFile(clientID int) error {
	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, sp.SerializeMsgEndOfFile(clientID))
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendReviewsEndOfFile(clientID int, negativeReviewsPreFiltersAmount int, reviewMappersAmount int) error {
	for i := 1; i <= negativeReviewsPreFiltersAmount; i++ {
		negativePreFilterRoutingKey := fmt.Sprintf("%s%d", NegativePreFilterRoutingKeyPrefix, i)
		err := m.NegativePreFilterExchange.Publish(negativePreFilterRoutingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
		fmt.Printf("Sent EOF to negative pre filter %d\n", i)
	}

	for i := 1; i <= reviewMappersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ReviewsRoutingKeyPrefix, i)
		err := m.ReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}
	}

	return nil
}

func (m *Middleware) ReceiveQueryResponse() ([]byte, error) {
	rawMsg, err := m.QueryResultsQueue.Consume()
	if err != nil {
		return nil, fmt.Errorf("failed to consume message: %v", err)
	}

	queryResponseMessage, err := sp.DeserializeQuery(rawMsg)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}
	// fmt.Printf("Received query response of type: %d\n", queryResponseMessage.Type)
	switch queryResponseMessage.Type {
	case sp.MsgOsResolvedQuery:
		fmt.Printf("Received OS resolved query\n")
		return handleMsgOsResolvedQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)

	case sp.MsgTopTenDecadeAvgPtfQuery:
		fmt.Printf("Received decade reviews query\n")
		return handleMsgTopTenResolvedQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)

	case sp.MsgIndiePositiveJoinedReviewsQuery:
		fmt.Printf("Received positive indie reviews query\n")
		return handleMsgIndiePositiveResolvedQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)

	case sp.MsgActionPositiveReviewsQuery:
		fmt.Printf("Received positive reviews query\n")
		return handleMsgActionPositiveReviewsQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)

	case sp.MsgActionNegativeReviewsQuery:
		fmt.Printf("Received negative reviews query\n")
		return handleMsgActionNegativeReviewsQuery(queryResponseMessage.ClientID, queryResponseMessage.Body)

	default:
		fmt.Printf("Received unknown query response\n")
	}
	return rawMsg, nil
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

func handleMsgActionPositiveReviewsQuery(clientID int, message []byte) ([]byte, error) {
	joinedReviews, err := sp.DeserializeMsgActionNegativeReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}

	var stringRepresentation []byte
	for _, review := range joinedReviews {
		stringRep := j.GetStrRepresentationNegativeGameReviewOnlyName(review)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(clientID), byte(sp.MsgActionPositiveReviewsQuery), stringRepresentation), nil
}

func handleMsgActionNegativeReviewsQuery(clientID int, message []byte) ([]byte, error) {
	joinedReviews, err := sp.DeserializeMsgActionNegativeReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}

	var stringRepresentation []byte
	for _, review := range joinedReviews {
		stringRep := j.GetStrRepresentationNegativeGameReview(review)
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
