package decade_filter

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/op/go-logging"
)

type GameYearAndAvgPtf struct {
	AppId              uint32
	ReleaseYear        uint16
	AvgPlaytimeForever uint32
}

var log = logging.MustGetLogger("log")

func SerializeGameYearAndAvgPtf(gameYearAndAvgPtf *GameYearAndAvgPtf) []byte {
	message := make([]byte, 10)
	binary.BigEndian.PutUint32(message[0:], gameYearAndAvgPtf.AppId)
	binary.BigEndian.PutUint16(message[4:], gameYearAndAvgPtf.ReleaseYear)
	binary.BigEndian.PutUint32(message[6:], gameYearAndAvgPtf.AvgPlaytimeForever)
	return message
}

func DeserializeGameYearAndAvgPtf(message []byte) (*GameYearAndAvgPtf, error) {
	if len(message) != 10 {
		return nil, nil
	}

	appId := binary.BigEndian.Uint32(message[0:])
	releaseYear := binary.BigEndian.Uint16(message[4:])
	avgPlayTimeForever := binary.BigEndian.Uint32(message[6:])

	return &GameYearAndAvgPtf{
		AppId:              appId,
		ReleaseYear:        releaseYear,
		AvgPlaytimeForever: avgPlayTimeForever,
	}, nil
}

func NewGameYearAndAvgPtf(appId string, releaseDate string, avgPlayTimeForever string) (*GameYearAndAvgPtf, error) {

	uint16ReleaseYear, err := ParseYearFromDate(releaseDate)
	if err != nil {
		log.Errorf("Error parsing ReleaseYear value: %v", err)
		return nil, err
	}

	uint16AppId, err := strconv.ParseUint(appId, 10, 32)
	if err != nil {
		log.Errorf("Error parsing AppId value: %v", err)
		return nil, err
	}

	uint32AvgPlayTimeForever, err := strconv.ParseUint(avgPlayTimeForever, 10, 32)
	if err != nil {
		log.Errorf("Error parsing AvgPlayTimeForever value: %v", err)
		return nil, err
	}

	return &GameYearAndAvgPtf{
		AppId:              uint32(uint16AppId),
		ReleaseYear:        uint16(uint16ReleaseYear),
		AvgPlaytimeForever: uint32(uint32AvgPlayTimeForever),
	}, nil
}

const Layout1 = "Jan 2, 2006"
const Layout2 = "Jan 2006"

func ParseYearFromDate(dateStr string) (uint16, error) {
	t, err := time.Parse(Layout1, dateStr)
	if err != nil {
		t, err = time.Parse(Layout2, dateStr)
		if err != nil {
			log.Errorf("Error parsing date: %v", err)
			return 0, err
		}
	}
	return uint16(t.Year()), nil
}

func FilterByDecade(gameYearAndAvgPtf []*GameYearAndAvgPtf, decade uint16) []*GameYearAndAvgPtf {
	var gamesInDecade []*GameYearAndAvgPtf
	for _, game := range gameYearAndAvgPtf {
		if game.ReleaseYear >= decade && game.ReleaseYear < decade+10 {
			gamesInDecade = append(gamesInDecade, game)
		}
	}
	return gamesInDecade
}
