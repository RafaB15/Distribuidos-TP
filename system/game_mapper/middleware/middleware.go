package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"
	RawGamesQueueName    = "raw_games_queue"

	OSGamesExchangeName = "os_games_exchange"
	OSGamesRoutingKey   = "os_games_key"
	OSGamesExchangeType = "direct"

	YearAndAvgPtfExchangeName = "year_and_avg_ptf_exchange"
	YearAndAvgPtfExchangeType = "direct"
	YearAndAvgPtfRoutingKey   = "year_and_avg_ptf_key"

	IndieReviewJoinExchangeName     = "indie_review_join_exchange"
	IndieReviewJoinExchangeType     = "direct"
	IndieReviewJoinRoutingKeyPrefix = "indie_key_"

	ActionReviewJoinExchangeName     = "action_review_join_exchange"
	ActionReviewJoinExchangeType     = "direct"
	ActionReviewJoinRoutingKeyPrefix = "action_key_"

	IndieGenre  = "indie"
	ActionGenre = "action"

	OSAcumulatorsAmountEnvironmentVariableName       = "OS_ACCUMULATORS_AMOUNT"
	DecadeFilterAmountEnvironmentVariableName        = "DECADE_FILTER_AMOUNT"
	IndieReviewJoinersAmountEnvironmentVariableName  = "INDIE_REVIEW_JOINERS_AMOUNT"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
)

type Middleware struct {
	Manager                  *mom.MiddlewareManager
	RawGamesQueue            *mom.Queue
	OSGamesExchange          *mom.Exchange
	YearAndAvgPtfExchange    *mom.Exchange
	IndieReviewJoinExchange  *mom.Exchange
	ActionReviewJoinExchange *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		return nil, err
	}

	rawGamesQueue, err := manager.CreateBoundQueue(RawGamesQueueName, RawGamesExchangeName, RawGamesExchangeType, RawGamesRoutingKey, true)
	if err != nil {
		return nil, err
	}

	osGamesExchange, err := manager.CreateExchange(OSGamesExchangeName, OSGamesExchangeType)
	if err != nil {
		return nil, err
	}

	yearAndAvgPtfExchange, err := manager.CreateExchange(YearAndAvgPtfExchangeName, YearAndAvgPtfExchangeType)
	if err != nil {
		return nil, err
	}

	indieReviewJoinExchange, err := manager.CreateExchange(IndieReviewJoinExchangeName, IndieReviewJoinExchangeType)
	if err != nil {
		return nil, err
	}

	actionReviewJoinExchange, err := manager.CreateExchange(ActionReviewJoinExchangeName, ActionReviewJoinExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                  manager,
		RawGamesQueue:            rawGamesQueue,
		OSGamesExchange:          osGamesExchange,
		YearAndAvgPtfExchange:    yearAndAvgPtfExchange,
		IndieReviewJoinExchange:  indieReviewJoinExchange,
		ActionReviewJoinExchange: actionReviewJoinExchange,
	}, nil
}

func (m *Middleware) ReceiveGameBatch() ([]string, bool, error) {
	msg, err := m.RawGamesQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	messageType, err := sp.DeserializeMessageType(msg)
	if err != nil {
		return nil, false, err
	}

	var lines []string

	switch messageType {
	case sp.MsgEndOfFile:
		return nil, true, nil
	case sp.MsgBatch:
		lines, err = sp.DeserializeBatch(msg)
		if err != nil {
			return nil, false, err
		}
	default:
		return nil, false, fmt.Errorf("unexpected message type: %d", messageType)
	}

	return lines, false, nil
}
