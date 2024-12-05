package game_mapper

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	g "distribuidos-tp/internal/system_protocol/games"
	n "distribuidos-tp/internal/system_protocol/node"
	u "distribuidos-tp/internal/utils"
	p "distribuidos-tp/system/game_mapper/persistence"
	"encoding/csv"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

const (
	AppIdIndex       = 0
	AppNameIndex     = 1
	WindowsIndex     = 2
	MacIndex         = 3
	LinuxIndex       = 4
	GenresIndex      = 5
	DateIndex        = 6
	AvgPlaytimeIndex = 7

	IndieGenre  = "indie"
	ActionGenre = "action"

	EntrypointAmount = 1

	AckBatchSize = 10
)

var log = logging.MustGetLogger("log")

type ReceiveGameBatchFunc func(messageTracker *n.MessageTracker) (clientID int, gameLines []string, eof bool, newMessage bool, delMessage bool, e error)
type SendGamesOSFunc func(clientID int, osAccumulatorsAmount int, gamesOS []*oa.GameOS, messageTracker *n.MessageTracker) error
type SendGameYearAndAvgPtfFunc func(clientID int, decadeFilterAmount int, gameYearAndAvgPtf []*df.GameYearAndAvgPtf, messageTracker *n.MessageTracker) error
type SendIndieGamesNamesFunc func(clientID int, indieGamesNames map[int][]*g.GameName, messageTracker *n.MessageTracker) error
type SendActionGamesFunc func(clientID int, actionGames []*g.Game, actionReviewJoinerAmount int, messageTracker *n.MessageTracker) error
type SendEndOfFileFunc func(clientID int, osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int, messageTracker *n.MessageTracker) error
type SendDeleteClientFunc func(clientID int, osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int) error
type AckLastMessageFunc func() error

type GameMapper struct {
	ReceiveGameBatch      ReceiveGameBatchFunc
	SendGamesOS           SendGamesOSFunc
	SendGameYearAndAvgPtf SendGameYearAndAvgPtfFunc
	SendIndieGamesNames   SendIndieGamesNamesFunc
	SendActionGames       SendActionGamesFunc
	SendEndOfFile         SendEndOfFileFunc
	SendDeleteClient      SendDeleteClientFunc
	AckLastMessage        AckLastMessageFunc
	logger                *logging.Logger
}

func NewGameMapper(
	receiveGameBatch ReceiveGameBatchFunc,
	sendGamesOS SendGamesOSFunc,
	sendGameYearAndAvgPtf SendGameYearAndAvgPtfFunc,
	sendIndieGamesNames SendIndieGamesNamesFunc,
	sendActionGames SendActionGamesFunc,
	sendEndOfFile SendEndOfFileFunc,
	deleteClient SendDeleteClientFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *GameMapper {
	return &GameMapper{
		ReceiveGameBatch:      receiveGameBatch,
		SendGamesOS:           sendGamesOS,
		SendGameYearAndAvgPtf: sendGameYearAndAvgPtf,
		SendIndieGamesNames:   sendIndieGamesNames,
		SendActionGames:       sendActionGames,
		SendEndOfFile:         sendEndOfFile,
		SendDeleteClient:      deleteClient,
		AckLastMessage:        ackLastMessage,
		logger:                logger,
	}
}

func (gm *GameMapper) Run(osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int, repository *p.Repository) {
	messageTracker, syncNumber := repository.LoadMessageTracker(EntrypointAmount)

	messagesUntilAck := AckBatchSize

	for {
		var gamesOS []*oa.GameOS
		var gamesYearAndAvgPtf []*df.GameYearAndAvgPtf

		indieGamesNames := make(map[int][]*g.GameName)
		actionGames := make([]*g.Game, 0)

		clientID, games, eof, newMessage, delMessage, err := gm.ReceiveGameBatch(messageTracker)
		if err != nil {
			log.Errorf("Failed to receive game batch: %v", err)
			return
		}

		if newMessage && !eof && !delMessage {

			for _, game := range games {
				records, err := getRecords(game)
				if err != nil {
					log.Errorf("Failed to read game: %v", err)
					continue
				}

				gamesOS, err = createAndAppendGameOS(records, gamesOS)
				if err != nil {
					log.Errorf("Failed to create game os struct: %v", err)
					continue
				}

				if belongsToGenre(IndieGenre, records) {
					gamesYearAndAvgPtf, err = createAndAppendGameYearAndAvgPtf(records, gamesYearAndAvgPtf)
					if err != nil {
						log.Errorf("Failed to create game year and avg ptf struct: %v", err)
						continue
					}
				}

				err = updateGameNameMaps(records, indieGamesNames, &actionGames, indieReviewJoinersAmount)
				if err != nil {
					log.Errorf("Failed to update game name maps: %v", err)
					continue
				}
			}

			err = gm.SendGamesOS(clientID, osAccumulatorsAmount, gamesOS, messageTracker)
			if err != nil {
				log.Errorf("Failed to send game os: %v", err)
				return
			}

			err = gm.SendGameYearAndAvgPtf(clientID, decadeFilterAmount, gamesYearAndAvgPtf, messageTracker)
			if err != nil {
				log.Errorf("Failed to send game year and avg ptf: %v", err)
				return
			}

			err = gm.SendIndieGamesNames(clientID, indieGamesNames, messageTracker)
			if err != nil {
				log.Errorf("Failed to send indie games names: %v", err)
				return
			}

			err = gm.SendActionGames(clientID, actionGames, actionReviewJoinersAmount, messageTracker)
			if err != nil {
				log.Errorf("Failed to send action games names: %v", err)
				return
			}
		}

		if delMessage {
			gm.logger.Infof("Deleting client %d information", clientID)

			err = gm.SendDeleteClient(clientID, osAccumulatorsAmount, decadeFilterAmount, indieReviewJoinersAmount, actionReviewJoinersAmount)
			if err != nil {
				log.Errorf("Failed to send delete client: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)

			syncNumber++
			err = repository.SaveMessageTracker(messageTracker, syncNumber)
			if err != nil {
				log.Errorf("Failed to save message tracker: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = gm.AckLastMessage()
			if err != nil {
				log.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messageTracker.ClientFinished(clientID, log) {
			log.Infof("Client %d finished", clientID)

			log.Info("Sending EOFs")
			err = gm.SendEndOfFile(clientID, osAccumulatorsAmount, decadeFilterAmount, indieReviewJoinersAmount, actionReviewJoinersAmount, messageTracker)
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)

			syncNumber++
			err = repository.SaveMessageTracker(messageTracker, syncNumber)
			if err != nil {
				log.Errorf("Failed to save message tracker: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = gm.AckLastMessage()
			if err != nil {
				log.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			syncNumber++
			err = repository.SaveMessageTracker(messageTracker, syncNumber)
			if err != nil {
				gm.logger.Errorf("Failed to save message tracker: %v", err)
				return
			}

			err = gm.AckLastMessage()
			if err != nil {
				gm.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}
	}
}

func getRecords(game string) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(game))
	records, err := reader.Read()

	if err != nil {
		log.Errorf("Failed to read game: %v", err)
		return nil, err
	}

	return records, nil
}

func createAndAppendGameOS(records []string, gameOsSlice []*oa.GameOS) ([]*oa.GameOS, error) {
	gameOs, err := oa.NewGameOS(records[WindowsIndex], records[MacIndex], records[LinuxIndex])
	if err != nil {
		log.Error("error creating game os struct")
		return gameOsSlice, err
	}

	gameOsSlice = append(gameOsSlice, gameOs)
	return gameOsSlice, nil
}

func createAndAppendGameYearAndAvgPtf(records []string, gameYearAndAvgPtfSlice []*df.GameYearAndAvgPtf) ([]*df.GameYearAndAvgPtf, error) {
	gameYearAndAvgPtf, err := df.NewGameYearAndAvgPtf(records[AppIdIndex], records[DateIndex], records[AvgPlaytimeIndex])
	if err != nil {
		log.Error("error creating game year and avg ptf struct")
		return gameYearAndAvgPtfSlice, err
	}

	gameYearAndAvgPtfSlice = append(gameYearAndAvgPtfSlice, gameYearAndAvgPtf)
	return gameYearAndAvgPtfSlice, nil
}

func belongsToGenre(genre string, record []string) bool {
	genres := strings.Split(record[GenresIndex], ",")

	for _, currentGenre := range genres {
		if currentGenre == genre {
			return true
		}
	}
	return false

}

func updateGameNameMaps(records []string, indieGamesNames map[int][]*g.GameName, actionGames *[]*g.Game, indieReviewJoinersAmount int) error {
	indie := false
	action := false

	appId, err := strconv.Atoi(records[AppIdIndex])
	if err != nil {
		log.Error("error converting appId")
		return err
	}

	gameName := g.NewGameName(uint32(appId), records[AppNameIndex])

	genres := strings.Split(records[GenresIndex], ",")

	for _, genre := range genres {
		genre = strings.TrimSpace(genre)
		switch genre {
		case IndieGenre:
			indie = true
		case ActionGenre:
			action = true
		}
	}

	if indie {
		indieShardingKey := u.CalculateShardingKey(records[AppIdIndex], indieReviewJoinersAmount)
		indieGamesNames[indieShardingKey] = append(indieGamesNames[indieShardingKey], gameName)
	}

	*actionGames = append(*actionGames, g.NewGame(uint32(appId), records[AppNameIndex], action, indie))

	return nil
}
