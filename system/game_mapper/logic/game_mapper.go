package game_mapper

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	g "distribuidos-tp/internal/system_protocol/games"
	n "distribuidos-tp/internal/system_protocol/node"
	u "distribuidos-tp/internal/utils"
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
)

var log = logging.MustGetLogger("log")

type ReceiveGameBatchFunc func(messageTracker *n.MessageTracker) (clientID int, gameLines []string, eof bool, newMessage bool, e error)
type SendGamesOSFunc func(clientID int, osAccumulatorsAmount int, gamesOS []*oa.GameOS, messageTracker *n.MessageTracker) error
type SendGameYearAndAvgPtfFunc func(clientID int, decadeFilterAmount int, gameYearAndAvgPtf []*df.GameYearAndAvgPtf, messageTracker *n.MessageTracker) error
type SendIndieGamesNamesFunc func(clientID int, indieGamesNames map[int][]*g.GameName, messageTracker *n.MessageTracker) error
type SendActionGamesFunc func(clientID int, actionGames []*g.Game, actionReviewJoinerAmount int, messageTracker *n.MessageTracker) error
type SendEndOfFileFunc func(clientID int, osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int, messageTracker *n.MessageTracker) error

type GameMapper struct {
	ReceiveGameBatch      ReceiveGameBatchFunc
	SendGamesOS           SendGamesOSFunc
	SendGameYearAndAvgPtf SendGameYearAndAvgPtfFunc
	SendIndieGamesNames   SendIndieGamesNamesFunc
	SendActionGames       SendActionGamesFunc
	SendEndOfFile         SendEndOfFileFunc
}

func NewGameMapper(
	receiveGameBatch ReceiveGameBatchFunc,
	sendGamesOS SendGamesOSFunc,
	sendGameYearAndAvgPtf SendGameYearAndAvgPtfFunc,
	sendIndieGamesNames SendIndieGamesNamesFunc,
	sendActionGames SendActionGamesFunc,
	sendEndOfFile SendEndOfFileFunc,
) *GameMapper {
	return &GameMapper{
		ReceiveGameBatch:      receiveGameBatch,
		SendGamesOS:           sendGamesOS,
		SendGameYearAndAvgPtf: sendGameYearAndAvgPtf,
		SendIndieGamesNames:   sendIndieGamesNames,
		SendActionGames:       sendActionGames,
		SendEndOfFile:         sendEndOfFile,
	}
}

func (gm *GameMapper) Run(osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int) {
	messageTracker := n.NewMessageTracker(EntrypointAmount)

	for {
		var gamesOS []*oa.GameOS
		var gamesYearAndAvgPtf []*df.GameYearAndAvgPtf

		indieGamesNames := make(map[int][]*g.GameName)
		actionGames := make([]*g.Game, 0)

		clientID, games, eof, newMessage, err := gm.ReceiveGameBatch(messageTracker)
		if err != nil {
			log.Errorf("Failed to receive game batch: %v", err)
			return
		}

		if newMessage && !eof {

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
				log.Infof("Action games in: %d", len(actionGames))

			}

			log.Infof("Action games out: %d", len(actionGames))

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

		if messageTracker.ClientFinished(clientID, log) {
			log.Infof("Client %d finished", clientID)

			log.Info("Sending EOFs")
			err = gm.SendEndOfFile(clientID, osAccumulatorsAmount, decadeFilterAmount, indieReviewJoinersAmount, actionReviewJoinersAmount, messageTracker)
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
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
