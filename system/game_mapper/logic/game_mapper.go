package game_mapper

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	g "distribuidos-tp/internal/system_protocol/games"
	u "distribuidos-tp/internal/utils"
	"encoding/csv"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

const (
	APP_ID_INDEX       = 0
	APP_NAME_INDEX     = 1
	WINDOWS_INDEX      = 2
	MAC_INDEX          = 3
	LINUX_INDEX        = 4
	GENRES_INDEX       = 5
	DATE_INDEX         = 6
	AVG_PLAYTIME_INDEX = 7

	IndieGenre  = "indie"
	ActionGenre = "action"
)

var log = logging.MustGetLogger("log")

type ReceiveGameBatchFunc func() (int, []string, bool, error)
type SendGamesOSFunc func(int, []*oa.GameOS) error
type SendGameYearAndAvgPtfFunc func([]*df.GameYearAndAvgPtf) error
type SendIndieGamesNamesFunc func(int, map[int][]*g.GameName) error
type SendActionGamesNamesFunc func(int, map[int][]*g.GameName) error
type SendEndOfFileFunc func(int, int, int, int, int) error

type GameMapper struct {
	ReceiveGameBatch      ReceiveGameBatchFunc
	SendGamesOS           SendGamesOSFunc
	SendGameYearAndAvgPtf SendGameYearAndAvgPtfFunc
	SendIndieGamesNames   SendIndieGamesNamesFunc
	SendActionGamesNames  SendActionGamesNamesFunc
	SendEndOfFile         SendEndOfFileFunc
}

func NewGameMapper(
	receiveGameBatch ReceiveGameBatchFunc,
	sendGamesOS SendGamesOSFunc,
	sendGameYearAndAvgPtf SendGameYearAndAvgPtfFunc,
	sendIndieGamesNames SendIndieGamesNamesFunc,
	sendActionGamesNames SendActionGamesNamesFunc,
	sendEndOfFile SendEndOfFileFunc,
) *GameMapper {
	return &GameMapper{
		ReceiveGameBatch:      receiveGameBatch,
		SendGamesOS:           sendGamesOS,
		SendGameYearAndAvgPtf: sendGameYearAndAvgPtf,
		SendIndieGamesNames:   sendIndieGamesNames,
		SendActionGamesNames:  sendActionGamesNames,
		SendEndOfFile:         sendEndOfFile,
	}
}

func (gm *GameMapper) Run(osAccumulatorsAmount int, decadeFilterAmount int, indieReviewJoinersAmount int, actionReviewJoinersAmount int) {

	for {
		var gamesOS []*oa.GameOS
		var gamesYearAndAvgPtf []*df.GameYearAndAvgPtf

		indieGamesNames := make(map[int][]*g.GameName)
		actionGamesNames := make(map[int][]*g.GameName)

		clientID, games, eof, err := gm.ReceiveGameBatch()
		if err != nil {
			log.Errorf("Failed to receive game batch: %v", err)
			return
		}

		if eof {
			log.Info("End of file received")
			gm.SendEndOfFile(clientID, osAccumulatorsAmount, decadeFilterAmount, indieReviewJoinersAmount, actionReviewJoinersAmount)
			continue
		}

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

			err = updateGameNameMaps(records, indieGamesNames, actionGamesNames, indieReviewJoinersAmount, actionReviewJoinersAmount)
			if err != nil {
				log.Errorf("Failed to update game name maps: %v", err)
				continue
			}
		}

		err = gm.SendGamesOS(clientID, gamesOS)
		if err != nil {
			log.Errorf("Failed to send game os: %v", err)
			return
		}

		err = gm.SendGameYearAndAvgPtf(gamesYearAndAvgPtf)
		if err != nil {
			log.Errorf("Failed to send game year and avg ptf: %v", err)
			return
		}

		err = gm.SendIndieGamesNames(clientID, indieGamesNames)
		if err != nil {
			log.Errorf("Failed to send indie games names: %v", err)
			return
		}

		err = gm.SendActionGamesNames(clientID, actionGamesNames)
		if err != nil {
			log.Errorf("Failed to send action games names: %v", err)
			return
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
	gameOs, err := oa.NewGameOS(records[WINDOWS_INDEX], records[MAC_INDEX], records[LINUX_INDEX])
	if err != nil {
		log.Error("error creating game os struct")
		return gameOsSlice, err
	}

	gameOsSlice = append(gameOsSlice, gameOs)
	return gameOsSlice, nil
}

func createAndAppendGameYearAndAvgPtf(records []string, gameYearAndAvgPtfSlice []*df.GameYearAndAvgPtf) ([]*df.GameYearAndAvgPtf, error) {
	gameYearAndAvgPtf, err := df.NewGameYearAndAvgPtf(records[APP_ID_INDEX], records[DATE_INDEX], records[AVG_PLAYTIME_INDEX])
	if err != nil {
		log.Error("error creating game year and avg ptf struct")
		return gameYearAndAvgPtfSlice, err
	}

	gameYearAndAvgPtfSlice = append(gameYearAndAvgPtfSlice, gameYearAndAvgPtf)
	return gameYearAndAvgPtfSlice, nil
}

func belongsToGenre(genre string, record []string) bool {
	genres := strings.Split(record[GENRES_INDEX], ",")

	for _, g := range genres {
		if g == genre {
			return true
		}
	}
	return false

}

func updateGameNameMaps(records []string, indieGamesNames map[int][]*g.GameName, actionGamesNames map[int][]*g.GameName, indieReviewJoinersAmount int, actionReviewJoinersAmount int) error {
	appId, err := strconv.Atoi(records[APP_ID_INDEX])
	if err != nil {
		log.Error("error converting appId")
		return err
	}

	gameName := g.NewGameName(uint32(appId), records[APP_NAME_INDEX])
	genres := strings.Split(records[GENRES_INDEX], ",")

	for _, genre := range genres {
		genre = strings.TrimSpace(genre)
		switch genre {
		case IndieGenre:
			indieShardingKey := u.CalculateShardingKey(records[APP_ID_INDEX], indieReviewJoinersAmount)
			indieGamesNames[indieShardingKey] = append(indieGamesNames[indieShardingKey], gameName)
		case ActionGenre:
			actionShardingKey := u.CalculateShardingKey(records[APP_ID_INDEX], actionReviewJoinersAmount)
			actionGamesNames[actionShardingKey] = append(actionGamesNames[actionShardingKey], gameName)
		}
	}

	return nil
}
