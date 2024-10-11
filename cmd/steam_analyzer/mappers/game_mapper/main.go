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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

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

	OSAcumulatorsAmountEnvironmentVariableName       = "OS_ACCUMULATORS_AMOUNT"
	DecadeFilterAmountEnvironmentVariableName        = "DECADE_FILTER_AMOUNT"
	IndieReviewJoinersAmountEnvironmentVariableName  = "INDIE_REVIEW_JOINERS_AMOUNT"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	osAccumulatorsAmount, err := u.GetEnvInt(OSAcumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	decadeFilterAmount, err := u.GetEnvInt(DecadeFilterAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	indieReviewJoinersAmount, err := u.GetEnvInt(IndieReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	actionReviewJoinersAmount, err := u.GetEnvInt(ActionReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

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

	go mapLines(rawGamesQueue, osGamesExchange, gameYearAndAvgPtfExchange, indieReviewJoinExchange, actionReviewJoinExchange, osAccumulatorsAmount, decadeFilterAmount, indieReviewJoinersAmount, actionReviewJoinersAmount)
	log.Info("Waiting for messages. To exit press CTRL+C")
	go func() {
		sig := <-sigs
		log.Infof("Received signal: %v. Waiting for tasks to complete...", sig)
		log.Info("All tasks completed. Shutting down.")
		done <- true
	}()

	<-done
	<-forever
}

func mapLines(rawGamesQueue *mom.Queue, osGamesExchange *mom.Exchange, gameYearAndAvgPtfExchange *mom.Exchange, indieReviewJoinExchange *mom.Exchange, actionReviewJoinExchange *mom.Exchange, osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int) error {
	msgs, err := rawGamesQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}
loop:
	for d := range msgs {

		msgType, err := sp.DeserializeMessageType(d.Body)
		if err != nil {
			return err
		}

		switch msgType {

		case sp.MsgEndOfFile:
			log.Info("End of file received")

			for i := 0; i < osAccumulatorsAmount; i++ {
				osGamesExchange.Publish(OSGamesRoutingKey, sp.SerializeMsgEndOfFile())
			}

			for i := 0; i < decadeFilterAmount; i++ {
				gameYearAndAvgPtfExchange.Publish(YearAndAvgPtfRoutingKey, sp.SerializeMsgEndOfFile())
			}

			for i := 1; i <= indieReviewJoinersAmount; i++ {
				routingKey := u.GetPartitioningKeyFromInt(i, indieReviewJoinersAmount, IndieReviewJoinRoutingKeyPrefix)
				indieReviewJoinExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
			}

			for i := 1; i <= actionReviewJoinersAmount; i++ {
				routingKey := u.GetPartitioningKeyFromInt(i, actionReviewJoinersAmount, ActionReviewJoinRoutingKeyPrefix)
				actionReviewJoinExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
			}

			break loop

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

				err = updateGameNameMaps(records, indieGamesNames, actionGamesNames, indieReviewJoinersAmount, actionReviewJoinersAmount)
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

func updateGameNameMaps(records []string, indieGamesNames map[string][]*g.GameName, actionGamesNames map[string][]*g.GameName, indieReviewJoinersAmount int, actionReviewJoinersAmount int) error {
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
			indiePartitioningKey := u.GetPartitioningKey(records[0], indieReviewJoinersAmount, IndieReviewJoinRoutingKeyPrefix)
			indieGamesNames[indiePartitioningKey] = append(indieGamesNames[indiePartitioningKey], gameName)
		case ActionGenre:
			actionPartitioningKey := u.GetPartitioningKey(records[0], actionReviewJoinersAmount, ActionReviewJoinRoutingKeyPrefix)
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
