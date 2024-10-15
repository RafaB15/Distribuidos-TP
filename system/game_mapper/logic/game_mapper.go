package game_mapper

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	"encoding/csv"
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

type GameMapper struct {
	ReceiveGameBatch func() ([]string, bool, error)
}

func NewGameMapper(receiveGameBatch func() ([]string, bool, error)) *GameMapper {
	return &GameMapper{
		ReceiveGameBatch: receiveGameBatch,
	}
}

func (g *GameMapper) Run() {

	for {
		var gameOsSlice []*oa.GameOS
		var gameYearAndAvgPtfSlice []*df.GameYearAndAvgPtf

		games, eof, err := g.ReceiveGameBatch()
		if err != nil {
			log.Errorf("Failed to receive game batch: %v", err)
			return
		}

		if eof {
			//Algo haremos
		}

		for _, game := range games {
			records, err := getRecords(game)
			if err != nil {
				log.Errorf("Failed to read game: %v", err)
				continue
			}

			gameOsSlice, err = createAndAppendGameOS(records, gameOsSlice)
			if err != nil {
				log.Errorf("Failed to create game os struct: %v", err)
				continue
			}

			if 
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

func belongsToGenre(genre string, record []string) bool {
	genres := strings.Split(record[GENRES_INDEX], ",")

	for _, g := range genres {
		if g == genre {
			return true
		}
	}
	return false

}
