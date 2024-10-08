package decade_filter

import (
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"io"
	"os"
	"sort"
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
		log.Errorf("Invalid game year ptf length: %d", len(message))
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

func TopTenAvgPlaytimeForever(games []*GameYearAndAvgPtf) []*GameYearAndAvgPtf {
	// Sort the slice in descending order by AvgPlaytimeForever
	sort.Slice(games, func(i, j int) bool {
		return games[i].AvgPlaytimeForever > games[j].AvgPlaytimeForever
	})

	// If there are less than or exactly 10 games, return the entire list
	if len(games) <= 10 {
		return games
	}

	// Otherwise, return only the top 10
	return games[:10]
}

func SaveTopTenAvgPlaytimeForeverToFile(games []*GameYearAndAvgPtf, filePath string) error {
	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		log.Errorf("Error creating file: %v", err)
		return err
	}
	defer file.Close()

	amount := len(games)
	result := make([]byte, 1+amount*10)

	result[0] = byte(amount)

	// Write the games to the slice without append
	for i, game := range games {
		gameBytes := SerializeGameYearAndAvgPtf(game)
		copy(result[1+i*10:], gameBytes)
	}

	// Write the result to the file
	_, err = file.Write(result)
	if err != nil {
		log.Errorf("Error writing game to file: %v", err)
		return err
	}

	return nil
}

func UploadTopTenAvgPlaytimeForeverFromFile(filePath string) ([]*GameYearAndAvgPtf, error) {

	var games []*GameYearAndAvgPtf

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		file, err := os.Create(filePath)
		if err != nil {
			log.Errorf("Error creating file: %v", err)
			return nil, err
		}
		file.Close()
	}
	file, err := os.Open(filePath)
	if err != nil {
		log.Errorf("Error opening file: %v", err)
		return nil, err
	}
	defer file.Close()

	bytesToRead, err := u.ReadExactFromFile(file, 1)
	if err != nil {
		if err == io.EOF {
			return games, nil
		}
		log.Errorf("Error reading file: %v", err)
		return nil, err
	}

	gamesBytes, err := u.ReadExactFromFile(file, int(bytesToRead[0])*10)
	if err != nil {
		log.Errorf("Error reading file: %v", err)
		return nil, err
	}

	// Deserialize the games
	for i := 0; i < int(bytesToRead[0]); i++ {
		game, err := DeserializeGameYearAndAvgPtf(gamesBytes[i*10 : (i+1)*10])
		if err != nil {
			log.Errorf("Error deserializing game: %v", err)
			return nil, err
		}
		games = append(games, game)
	}
	return games, nil
}
