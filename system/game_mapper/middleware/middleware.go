package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	g "distribuidos-tp/internal/system_protocol/games"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"
	RawGamesQueueName    = "raw_games_queue"

	OSGamesExchangeName     = "os_games_exchange"
	OSGamesRoutingKeyPrefix = "os_games_key_"
	OSGamesExchangeType     = "direct"

	YearAndAvgPtfExchangeName = "year_avg_ptf_exchange"
	YearAndAvgPtfExchangeType = "direct"
	YearAndAvgPtfRoutingKey   = "year_avg_ptf_key"

	IndieReviewJoinExchangeName     = "indie_review_join_exchange"
	IndieReviewJoinExchangeType     = "direct"
	IndieReviewJoinRoutingKeyPrefix = "indie_key_"

	ActionReviewJoinExchangeName     = "action_review_join_exchange"
	ActionReviewJoinExchangeType     = "direct"
	ActionReviewJoinRoutingKeyPrefix = "action_key_"

	IndieGenre  = "indie"
	ActionGenre = "action"
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

func (m *Middleware) ReceiveGameBatch() (int, []string, bool, error) {
	rawMsg, err := m.RawGamesQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, err
	}

	var lines []string

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	case sp.MsgBatch:
		lines, err = sp.DeserializeMsgBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}
	default:
		return message.ClientID, nil, false, fmt.Errorf("unexpected message type: %d", message.Type)
	}

	return message.ClientID, lines, false, nil
}

func (m *Middleware) SendGamesOS(clientID int, osAccumulatorsAmount int, gamesOS []*oa.GameOS) error {
	serializedGameOS := sp.SerializeMsgGameOSInformation(clientID, gamesOS)

	randomNode := u.GetRandomNumber(osAccumulatorsAmount)

	routingKey := fmt.Sprintf("%s%d", OSGamesRoutingKeyPrefix, randomNode)

	fmt.Printf("Publishing games OS to routingKey: %s for clientID: %d\n", routingKey, clientID)
	err := m.OSGamesExchange.Publish(routingKey, serializedGameOS)
	if err != nil {
		return fmt.Errorf("failed to publish games OS: %v", err)
	}

	return nil
}

func (m *Middleware) SendGameYearAndAvgPtf(clientID int, gameYearAndAvgPtf []*df.GameYearAndAvgPtf) error {
	serializedGameYearAndAvgPtf := sp.SerializeMsgGameYearAndAvgPtf(clientID, gameYearAndAvgPtf)
	err := m.YearAndAvgPtfExchange.Publish(YearAndAvgPtfRoutingKey, serializedGameYearAndAvgPtf)
	if err != nil {
		return fmt.Errorf("failed to publish game year and avg ptf: %v", err)
	}

	return nil
}

func (m *Middleware) SendIndieGamesNames(clientID int, indieGamesNames map[int][]*g.GameName) error {
	return sendGamesNamesToReviewJoin(clientID, indieGamesNames, m.IndieReviewJoinExchange, IndieReviewJoinRoutingKeyPrefix)
}

func (m *Middleware) SendActionGamesNames(clientID int, actionGamesNames map[int][]*g.GameName) error {
	return sendGamesNamesToReviewJoin(clientID, actionGamesNames, m.ActionReviewJoinExchange, ActionReviewJoinRoutingKeyPrefix)
}

func sendGamesNamesToReviewJoin(clientID int, gamesNamesMap map[int][]*g.GameName, reviewJoinExchange *mom.Exchange, keyPrefix string) error {
	for shardingKey, gameName := range gamesNamesMap {
		routingKey := fmt.Sprintf("%s%d", keyPrefix, shardingKey)

		serializedGamesNames, err := sp.SerializeMsgGameNames(clientID, gameName)
		if err != nil {
			return fmt.Errorf("failed to serialize game names: %v", err)
		}

		err = reviewJoinExchange.Publish(routingKey, serializedGamesNames)
		if err != nil {
			return fmt.Errorf("failed to publish game names: %v", err)
		}
	}

	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int, osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int) error {
	for i := 0; i < osAccumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", OSGamesRoutingKeyPrefix, i+1)
		fmt.Printf("Publishing EndOfFile to routingKey: %s for clientID: %d\n", routingKey, clientID)
		err := m.OSGamesExchange.Publish(routingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return err
		}
	}

	for i := 0; i < decadeFilterAmount; i++ {
		err := m.YearAndAvgPtfExchange.Publish(YearAndAvgPtfRoutingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return err
		}
	}

	for i := 1; i <= indieReviewJoinersAmount; i++ {
		routingKey := u.GetPartitioningKeyFromInt(i, indieReviewJoinersAmount, IndieReviewJoinRoutingKeyPrefix)
		err := m.IndieReviewJoinExchange.Publish(routingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return err
		}
	}

	for i := 1; i <= actionReviewJoinersAmount; i++ {
		routingKey := u.GetPartitioningKeyFromInt(i, actionReviewJoinersAmount, ActionReviewJoinRoutingKeyPrefix)
		err := m.ActionReviewJoinExchange.Publish(routingKey, sp.SerializeMsgEndOfFile(clientID))
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
