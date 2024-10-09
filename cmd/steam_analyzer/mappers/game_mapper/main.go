package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	g "distribuidos-tp/internal/system_protocol/games"
	gf "distribuidos-tp/internal/system_protocol/genre_filter"
	u "distribuidos-tp/internal/utils"
	"encoding/csv"
	"io"
	"strconv"
	"strings"

	"github.com/op/go-logging"
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

	OSAccumulatorNodes    = 2
	DecadeFilterNodes     = 3
	IndieReviewJoinNodes  = 1
	ActionReviewJoinNodes = 1
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	rawGamesQueue, err := manager.CreateBoundQueue(RawGamesQueueName, RawGamesExchangeName, RawGamesExchangeType, RawGamesRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	osGamesExchange, err := manager.CreateExchange(OSGamesExchangeName, OSGamesExchangeType)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}

	gameYearAndAvgPtfExchange, err := manager.CreateExchange(YearAndAvgPtfExchangeName, YearAndAvgPtfExchangeType)
	if err != nil {
		log.Errorf("Failed to bind accumulator queue: %v", err)
		return
	}

	indieReviewJoinExchange, err := manager.CreateExchange(IndieReviewJoinExchangeName, IndieReviewJoinExchangeType)
	if err != nil {
		log.Errorf("Failed to create exchange: %v", err)
		return
	}

	actionReviewJoinExchange, err := manager.CreateExchange(ActionReviewJoinExchangeName, ActionReviewJoinExchangeType)
	if err != nil {
		log.Errorf("Failed to create exchange: %v", err)
		return
	}

	forever := make(chan bool)

	go mapLines(rawGamesQueue, osGamesExchange, gameYearAndAvgPtfExchange, indieReviewJoinExchange, actionReviewJoinExchange)
	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func mapLines(rawGamesQueue *mom.Queue, osGamesExchange *mom.Exchange, gameYearAndAvgPtfExchange *mom.Exchange, indieReviewJoinExchange *mom.Exchange, actionReviewJoinExchange *mom.Exchange) error {
	msgs, err := rawGamesQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}
	for d := range msgs {

		msgType, err := sp.DeserializeMessageType(d.Body)
		if err != nil {
			return err
		}

		switch msgType {

		case sp.MsgEndOfFile:
			log.Info("End of file received")

			for i := 0; i < OSAccumulatorNodes; i++ {
				osGamesExchange.Publish(OSGamesRoutingKey, sp.SerializeMsgEndOfFile())
			}

			for i := 0; i < DecadeFilterNodes; i++ {
				gameYearAndAvgPtfExchange.Publish(YearAndAvgPtfRoutingKey, sp.SerializeMsgEndOfFile())
			}

		case sp.MsgBatch:

			lines, err := sp.DeserializeBatch(d.Body)
			if err != nil {
				log.Error("Error deserializing batch")
				return err
			}

			var gameOsSlice []*oa.GameOS
			var gameYearAndAvgPtfSlice []*df.GameYearAndAvgPtf

			indieGamesNames := make(map[string][]*g.GameName)
			actionGamesNames := make(map[string][]*g.GameName)

			for _, line := range lines {
				reader := csv.NewReader(strings.NewReader(line))
				records, err := reader.Read()

				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				log.Debugf("Printing fields: 0 : %v, 1 : %v, 2 : %v, 3 : %v, 4 : %v", records[0], records[1], records[2], records[3], records[4])

				gameOsSlice, err = createAndAppendGameOS(records, gameOsSlice)
				if err != nil {
					log.Error("error creating and appending game os struct")
					return err
				}

				if gf.BelongsToGenre(IndieGenre, records) {
					gameYearAndAvgPtfSlice, err = createAndAppendGameYearAndAvgPtf(records, gameYearAndAvgPtfSlice)
					if err != nil {
						log.Error("error creating and appending game year and avg ptf struct")
						return err
					}
				}

				err = updateGameNameMaps(records, indieGamesNames, actionGamesNames)
				if err != nil {
					log.Errorf("error updating game name maps, %v", err)
					return err
				}
			}

			serializedGameOS := sp.SerializeMsgGameOSInformation(gameOsSlice)
			serializedGameYearAndAvgPtf := sp.SerializeMsgGameYearAndAvgPtf(gameYearAndAvgPtfSlice)

			err = osGamesExchange.Publish(OSGamesRoutingKey, serializedGameOS)
			if err != nil {
				log.Error("Error publishing game")
				return err
			}

			err = gameYearAndAvgPtfExchange.Publish(YearAndAvgPtfRoutingKey, serializedGameYearAndAvgPtf)
			if err != nil {
				log.Error("Error publishing game")
				return err
			}

			err = sendGamesNamesToReviewJoin(indieGamesNames, indieReviewJoinExchange)
			if err != nil {
				log.Error("error sending games names to review join")
				return err
			}

			err = sendGamesNamesToReviewJoin(actionGamesNames, actionReviewJoinExchange)
			if err != nil {
				log.Error("error sending games names to review join")
				return err
			}

			log.Infof("Received a message (after attempted send): %s\n", string(d.Body))

		}

	}

	return nil
}

func createAndAppendGameOS(records []string, gameOsSlice []*oa.GameOS) ([]*oa.GameOS, error) {
	gameOs, err := oa.NewGameOS(records[2], records[3], records[4])
	if err != nil {
		log.Error("error creating game os struct")
		return gameOsSlice, err
	}

	gameOsSlice = append(gameOsSlice, gameOs)
	return gameOsSlice, nil
}

func createAndAppendGameYearAndAvgPtf(records []string, gameYearAndAvgPtfSlice []*df.GameYearAndAvgPtf) ([]*df.GameYearAndAvgPtf, error) {
	gameYearAndAvgPtf, err := df.NewGameYearAndAvgPtf(records[0], records[6], records[7])
	if err != nil {
		log.Error("error creating game year and avg ptf struct")
		return gameYearAndAvgPtfSlice, err
	}

	gameYearAndAvgPtfSlice = append(gameYearAndAvgPtfSlice, gameYearAndAvgPtf)
	return gameYearAndAvgPtfSlice, nil
}

func updateGameNameMaps(records []string, indieGamesNames map[string][]*g.GameName, actionGamesNames map[string][]*g.GameName) error {
	appId, err := strconv.Atoi(records[0])
	if err != nil {
		log.Error("error converting appId")
		return err
	}

	gameName := g.NewGameName(uint32(appId), records[1])
	genres := strings.Split(records[5], ",")

	for _, genre := range genres {
		genre = strings.TrimSpace(genre)
		switch genre {
		case IndieGenre:
			indiePartitioningKey := u.GetPartitioningKey(records[0], IndieReviewJoinNodes, IndieReviewJoinRoutingKeyPrefix)
			indieGamesNames[indiePartitioningKey] = append(indieGamesNames[indiePartitioningKey], gameName)
		case ActionGenre:
			actionPartitioningKey := u.GetPartitioningKey(records[0], ActionReviewJoinNodes, ActionReviewJoinRoutingKeyPrefix)
			actionGamesNames[actionPartitioningKey] = append(actionGamesNames[actionPartitioningKey], gameName)
		}
	}

	return nil
}

func sendGamesNamesToReviewJoin(gamesNamesMap map[string][]*g.GameName, reviewJoinExchange *mom.Exchange) error {
	for key, gameName := range gamesNamesMap {
		serializedGamesNames, err := sp.SerializeMsgGameNames(gameName)
		if err != nil {
			log.Error("error serializing game name")
			return err
		}

		err = reviewJoinExchange.Publish(key, serializedGamesNames)
		if err != nil {
			log.Error("error publishing game name")
			return err
		}
	}

	return nil
}
