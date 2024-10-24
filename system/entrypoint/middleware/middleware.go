package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	j "distribuidos-tp/internal/system_protocol/joiner"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"

	RawReviewsExchangeName     = "raw_reviews_exchange"
	RawReviewsExchangeType     = "direct"
	RawReviewsRoutingKeyPrefix = "raw_reviews_key_"
	RawEnglishReviewsKeyPrefix = "raw_english_reviews_key_"

	QueryResultsQueueName    = "query_results_queue"
	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager            *mom.MiddlewareManager
	RawGamesExchange   *mom.Exchange
	RawReviewsExchange *mom.Exchange
	QueryResultsQueue  *mom.Queue
}

func NewMiddleware(clientID int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		return nil, fmt.Errorf("Failed to create middleware manager: %v", err)
	}

	rawGamesExchange, err := manager.CreateExchange(RawGamesExchangeName, RawGamesExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	rawReviewsExchange, err := manager.CreateExchange(RawReviewsExchangeName, RawReviewsExchangeType)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %v", err)
	}

	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	queryResultsQueue, err := manager.CreateBoundQueue(QueryResultsQueueName, QueryResultsExchangeName, QueryExchangeType, routingKey, true)
	if err != nil {
		return nil, fmt.Errorf("Failed to create queue: %v", err)
	}

	return &Middleware{
		Manager:            manager,
		RawGamesExchange:   rawGamesExchange,
		RawReviewsExchange: rawReviewsExchange,
		QueryResultsQueue:  queryResultsQueue,
	}, nil
}

func (m *Middleware) SendGamesBatch(clientID int, data []byte) error {
	batch := sp.SerializeMsgBatch(clientID, data)
	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, batch)
	if err != nil {
		return fmt.Errorf("Failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendReviewsBatch(clientID int, englishFiltersAmount int, reviewMappersAmount int, data []byte) error {
	reviewsBatch := sp.SerializeMsgBatch(clientID, data)

	err := sendToReviewNode(englishFiltersAmount, m.RawReviewsExchange, RawEnglishReviewsKeyPrefix, reviewsBatch)
	if err != nil {
		return fmt.Errorf("Failed to publish message to english review accumulator: %v", err)
	}

	err = sendToReviewNode(reviewMappersAmount, m.RawReviewsExchange, RawReviewsRoutingKeyPrefix, reviewsBatch)
	if err != nil {
		return fmt.Errorf("Failed to publish message to review mapper: %v", err)
	}

	return nil
}

func sendToReviewNode(nodesAmount int, exchange *mom.Exchange, routingKeyPrefix string, data []byte) error {
	nodeId := u.GetRandomNumber(nodesAmount)
	routingKey := fmt.Sprintf("%s%d", routingKeyPrefix, nodeId)
	err := exchange.Publish(routingKey, data)
	if err != nil {
		return fmt.Errorf("Failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendGamesEndOfFile(clientID int) error {
	err := m.RawGamesExchange.Publish(RawGamesRoutingKey, sp.SerializeMsgEndOfFileV2(clientID))
	if err != nil {
		return fmt.Errorf("Failed to publish message: %v", err)
	}

	return nil
}

func (m *Middleware) SendReviewsEndOfFile(clientID int, englishFiltersAmount int, reviewMappersAmount int) error {
	for i := 1; i <= englishFiltersAmount; i++ {
		englishRoutingKey := fmt.Sprintf("%s%d", RawEnglishReviewsKeyPrefix, i)
		err := m.RawReviewsExchange.Publish(englishRoutingKey, sp.SerializeMsgEndOfFileV2(clientID))
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	for i := 1; i <= reviewMappersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", RawReviewsRoutingKeyPrefix, i)
		err := m.RawReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFileV2(clientID))
		if err != nil {
			return fmt.Errorf("Failed to publish message: %v", err)
		}
	}

	return nil
}

func (m *Middleware) ReceiveQueryResponse() ([]byte, error) {
	rawMsg, err := m.QueryResultsQueue.Consume()
	if err != nil {
		return nil, fmt.Errorf("Failed to consume message: %v", err)
	}

	queryResponseMessage, err := sp.DeserializeQuery(rawMsg)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}
	// fmt.Printf("Received query response of type: %d\n", queryResponseMessage.Type)
	switch queryResponseMessage.Type {
	case sp.MsgOsResolvedQuery:
		fmt.Printf("Received OS resolved query\n")
		return handleMsgOsResolvedQuery(queryResponseMessage.Body)
	case sp.MsgIndiePositiveJoinedReviewsQuery:
		fmt.Printf("Received positive indie reviews query\n")
		return handleMsgIndiePositiveResolvedQuery(queryResponseMessage.Body)
	case sp.MsgActionPositiveReviewsQuery:
		fmt.Printf("Received positive reviews query\n")
		return handleMsgActionPositiveReviewsQuery(queryResponseMessage.Body)
	case sp.MsgActionNegativeReviewsQuery:
		fmt.Printf("Received negative reviews query\n")
		return handleMsgActionNegativeReviewsQuery(queryResponseMessage.Body)

	case sp.MsgTopTenDecadeAvgPtfQuery:
		return handleMsgTopTenResolvedQuery(queryResponseMessage.Body)

	}

	return rawMsg, nil
}

func handleMsgOsResolvedQuery(message []byte) ([]byte, error) {
	gameOSMetrics, err := sp.DeserializeMsgOsResolvedQuery(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize message: %v", err)
	}
	stringRepresentation := oa.GetStrRepresentation(gameOSMetrics)

	// fmt.Println("String Representation:", stringRepresentation)
	return sp.AssembleFinalQueryMsg(byte(sp.MsgOsResolvedQuery), []byte(stringRepresentation)), nil
}

func handleMsgIndiePositiveResolvedQuery(message []byte) ([]byte, error) {
	joinedReviews, err := sp.DeserializeMsgIndiePositiveJoinedReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize message: %v", err)
	}
	var stringRepresentation []byte
	for _, review := range joinedReviews {
		stringRep := j.GetStrRepresentation(review)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(sp.MsgIndiePositiveJoinedReviewsQuery), []byte(stringRepresentation)), nil
}

func handleMsgActionPositiveReviewsQuery(message []byte) ([]byte, error) {
	joinedReviews, err := sp.DeserializeMsgActionPositiveReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize message: %v", err)
	}

	var stringRepresentation []byte
	for _, review := range joinedReviews {
		stringRep := j.GetStrRepresentation(review)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(sp.MsgActionPositiveReviewsQuery), []byte(stringRepresentation)), nil
}

func handleMsgActionNegativeReviewsQuery(message []byte) ([]byte, error) {
	joinedReviews, err := sp.DeserializeMsgActionNegativeReviewsQuery(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize message: %v", err)
	}

	var stringRepresentation []byte
	for _, review := range joinedReviews {
		stringRep := j.GetStrRepresentationNegativeGameReview(review)
		stringRepresentation = append(stringRepresentation, []byte(stringRep)...)
	}
	return sp.AssembleFinalQueryMsg(byte(sp.MsgActionNegativeReviewsQuery), []byte(stringRepresentation)), nil

}

func handleMsgTopTenResolvedQuery(message []byte) ([]byte, error) {
	decadeAvgPtfs, err := sp.DeserializeMsgTopTenResolvedQuery(message)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}
	stringRepresentation := ""
	for _, decadeAvgPtf := range decadeAvgPtfs {
		stringRepresentation += df.GetStrRepresentation(decadeAvgPtf)

	}
	// fmt.Println("String Representation:", stringRepresentation)

	return sp.AssembleFinalQueryMsg(byte(sp.MsgTopTenDecadeAvgPtfQuery), []byte(stringRepresentation)), nil

}
