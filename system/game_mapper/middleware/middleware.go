package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	g "distribuidos-tp/internal/system_protocol/games"
	n "distribuidos-tp/internal/system_protocol/node"
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
	RawGamesQueueName    = "raw_games_queue"

	OSGamesExchangeName     = "os_games_exchange"
	OSGamesRoutingKeyPrefix = "os_games_key_"
	OSGamesExchangeType     = "direct"

	YearAndAvgPtfExchangeName     = "year_avg_ptf_exchange"
	YearAndAvgPtfExchangeType     = "direct"
	YearAndAvgPtfRoutingKeyPrefix = "year_avg_ptf_key_"

	IndieReviewJoinExchangeName     = "indie_review_join_exchange"
	IndieReviewJoinExchangeType     = "direct"
	IndieReviewJoinRoutingKeyPrefix = "indie_key_"

	ActionReviewJoinerExchangeName     = "action_review_joiner_exchange"
	ActionReviewJoinerExchangeType     = "direct"
	ActionReviewJoinerRoutingKeyPrefix = "action_review_joiner_key_"
	ActionReviewJoinerExchangePriority = 1
)

type Middleware struct {
	Manager                  *mom.MiddlewareManager
	RawGamesQueue            *mom.Queue
	OSGamesExchange          *mom.Exchange
	YearAndAvgPtfExchange    *mom.Exchange
	IndieReviewJoinExchange  *mom.Exchange
	ActionReviewJoinExchange *mom.Exchange
	logger                   *logging.Logger
}

func NewMiddleware(logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		return nil, err
	}

	rawGamesQueue, err := manager.CreateBoundQueue(RawGamesQueueName, RawGamesExchangeName, RawGamesExchangeType, RawGamesRoutingKey, false)
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

	actionReviewJoinExchange, err := manager.CreateExchange(ActionReviewJoinerExchangeName, ActionReviewJoinerExchangeType)
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
		logger:                   logger,
	}, nil
}

func (m *Middleware) ReceiveGameBatch(messageTracker *n.MessageTracker) (clientID int, gameLines []string, eof bool, newMessage bool, delMessage bool, e error) {
	rawMsg, err := m.RawGamesQueue.Consume()
	if err != nil {
		return 0, nil, false, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, false, false, err
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
			return message.ClientID, nil, false, false, false, err
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, false, err
		}

		return message.ClientID, nil, true, true, false, nil
	case sp.MsgDeleteClient:
		m.logger.Infof("Received delete client message from client %d", message.ClientID)
		return message.ClientID, nil, false, true, true, nil
	case sp.MsgBatch:
		lines, err := sp.DeserializeMsgBatch(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, err
		}
		return message.ClientID, lines, false, true, false, nil
	default:
		return message.ClientID, nil, false, false, false, fmt.Errorf("unexpected message type: %d", message.Type)
	}
}

func (m *Middleware) SendGamesOS(clientID int, osAccumulatorsAmount int, gamesOS []*oa.GameOS, messageTracker *n.MessageTracker) error {
	serializedGameOS := sp.SerializeMsgGameOSInformation(clientID, gamesOS)

	hashedSerializedGameOS, err := u.Hash(serializedGameOS)
	if err != nil {
		return fmt.Errorf("failed to hash serialized game OS: %v", err)
	}

	routingKey := u.GetPartitioningKeyFromInt(hashedSerializedGameOS, osAccumulatorsAmount, OSGamesRoutingKeyPrefix)

	m.logger.Infof("Publishing games OS to routingKey: %s for clientID: %d", routingKey, clientID)
	err = m.OSGamesExchange.Publish(routingKey, serializedGameOS)
	if err != nil {
		return fmt.Errorf("failed to publish games OS: %v", err)
	}
	messageTracker.RegisterSentMessage(clientID, routingKey)

	return nil
}

func (m *Middleware) SendGameYearAndAvgPtf(clientID int, decadeFilterAmount int, gameYearAndAvgPtf []*df.GameYearAndAvgPtf, messageTracker *n.MessageTracker) error {
	serializedGameYearAndAvgPtf := sp.SerializeMsgGameYearAndAvgPtf(clientID, gameYearAndAvgPtf)

	hashedSerializedGameYearAndAvgPtf, err := u.Hash(serializedGameYearAndAvgPtf)
	if err != nil {
		return fmt.Errorf("failed to hash serialized game year and avg ptf: %v", err)
	}

	routingKey := u.GetPartitioningKeyFromInt(hashedSerializedGameYearAndAvgPtf, decadeFilterAmount, YearAndAvgPtfRoutingKeyPrefix)
	m.logger.Infof("Publishing games OS to routingKey: %s for clientID: %d\n", routingKey, clientID)

	err = m.YearAndAvgPtfExchange.Publish(routingKey, serializedGameYearAndAvgPtf)
	if err != nil {
		return fmt.Errorf("failed to publish game year and avg ptf: %v", err)
	}
	messageTracker.RegisterSentMessage(clientID, routingKey)
	return nil
}

func (m *Middleware) SendIndieGamesNames(clientID int, indieGamesNames map[int][]*g.GameName, messageTracker *n.MessageTracker) error {
	for shardingKey, gameName := range indieGamesNames {
		routingKey := fmt.Sprintf("%s%d", IndieReviewJoinRoutingKeyPrefix, shardingKey)

		serializedGamesNames, err := sp.SerializeMsgGameNames(clientID, gameName)
		if err != nil {
			return fmt.Errorf("failed to serialize game names: %v", err)
		}

		err = m.IndieReviewJoinExchange.Publish(routingKey, serializedGamesNames)
		if err != nil {
			return fmt.Errorf("failed to publish game names: %v", err)
		}

		messageTracker.RegisterSentMessage(clientID, routingKey)
	}

	return nil
}

func (m *Middleware) SendActionGames(clientID int, actionGames []*g.Game, actionReviewJoinerAmount int, messageTracker *n.MessageTracker) error {
	routingKeyMap := make(map[string][]*g.Game)

	for _, game := range actionGames {
		routingKey := u.GetPartitioningKeyFromInt(int(game.AppId), actionReviewJoinerAmount, ActionReviewJoinerRoutingKeyPrefix)
		routingKeyMap[routingKey] = append(routingKeyMap[routingKey], game)
	}

	for routingKey, games := range routingKeyMap {
		serializedGames, err := sp.SerializeMsgGames(clientID, games)
		if err != nil {
			return fmt.Errorf("failed to serialize games: %v", err)
		}

		err = m.ActionReviewJoinExchange.PublishWithPriority(routingKey, serializedGames, ActionReviewJoinerExchangePriority)
		if err != nil {
			return fmt.Errorf("failed to publish games: %v", err)
		}

		messageTracker.RegisterSentMessage(clientID, routingKey)
		m.logger.Infof("Published games to action review joiner with routing key: %s", routingKey)
	}

	return nil
}

func (m *Middleware) SendEndOfFiles(clientID int, osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)

	for i := 1; i <= osAccumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", OSGamesRoutingKeyPrefix, i)
		messageSentToRoutingKey := messagesSent[routingKey]
		fmt.Printf("Publishing EndOfFile to routingKey: %s for clientID: %d\n", routingKey, clientID)
		serializedMessage := sp.SerializeMsgEndOfFile(clientID, 1, messageSentToRoutingKey)
		err := m.OSGamesExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			return err
		}
	}

	for i := 1; i <= decadeFilterAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", YearAndAvgPtfRoutingKeyPrefix, i)
		messageSentToRoutingKey := messagesSent[routingKey]
		fmt.Printf("Publishing EndOfFile to routingKey: %s for clientID: %d\n", routingKey, clientID)
		serializedMessage := sp.SerializeMsgEndOfFile(clientID, 1, messageSentToRoutingKey)
		err := m.YearAndAvgPtfExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			return err
		}
	}

	for i := 1; i <= indieReviewJoinersAmount; i++ {
		routingKey := u.GetPartitioningKeyFromInt(i, indieReviewJoinersAmount, IndieReviewJoinRoutingKeyPrefix)
		messageSentToRoutingKey := messagesSent[routingKey]
		serializedMessage := sp.SerializeMsgEndOfFile(clientID, 1, messageSentToRoutingKey)

		err := m.IndieReviewJoinExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			return err
		}
	}

	for i := 1; i <= actionReviewJoinersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ActionReviewJoinerRoutingKeyPrefix, i)
		messagesSentToReviewJoin := messagesSent[routingKey]
		serializedMsg := sp.SerializeMsgEndOfFile(clientID, 1, messagesSentToReviewJoin)
		err := m.ActionReviewJoinExchange.Publish(routingKey, serializedMsg)
		if err != nil {
			return err
		}
		m.logger.Infof("Published EOF to action review joiner %d with sent messages %d and routing key %s", i, messagesSentToReviewJoin, routingKey)
	}

	return nil
}

func (m *Middleware) AckLastMessages() error {
	err := m.RawGamesQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	m.logger.Infof("Acked last message")
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}

func (m *Middleware) SendDeleteClient(clientID int, osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int) error {
	serializedMessage := sp.SerializeMsgDeleteClient(clientID)

	for i := 1; i <= osAccumulatorsAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", OSGamesRoutingKeyPrefix, i)
		err := m.OSGamesExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			m.logger.Errorf("Failed to publish delete client message to routing key %s: %v", routingKey, err)
			return err
		}
	}

	for i := 1; i <= decadeFilterAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", YearAndAvgPtfRoutingKeyPrefix, i)
		err := m.YearAndAvgPtfExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			m.logger.Errorf("Failed to publish delete client message to routing key %s: %v", routingKey, err)
			return err
		}
	}

	for i := 1; i <= indieReviewJoinersAmount; i++ {
		routingKey := u.GetPartitioningKeyFromInt(i, indieReviewJoinersAmount, IndieReviewJoinRoutingKeyPrefix)
		err := m.IndieReviewJoinExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			m.logger.Errorf("Failed to publish delete client message to routing key %s: %v", routingKey, err)
			return err
		}
	}

	for i := 1; i <= actionReviewJoinersAmount; i++ {
		routingKey := fmt.Sprintf("%s%d", ActionReviewJoinerRoutingKeyPrefix, i)
		err := m.ActionReviewJoinExchange.Publish(routingKey, serializedMessage)
		if err != nil {
			m.logger.Errorf("Failed to publish delete client message to routing key %s: %v", routingKey, err)
			return err
		}
	}

	m.logger.Infof("Published delete client message for client %d", clientID)
	return nil
}
