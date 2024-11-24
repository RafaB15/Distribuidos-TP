package os_accumulator

import (
	"encoding/binary"
	"errors"
	"os"
	"strconv"
)

type GameOSMetrics struct {
	Linux   uint32
	Windows uint32
	Mac     uint32
}

func NewGameOSMetrics() *GameOSMetrics {

	return &GameOSMetrics{
		Linux:   0,
		Windows: 0,
		Mac:     0,
	}
}

const filePermission = 0644

func (g *GameOSMetrics) UpdateAndSaveGameOSMetricsToFile(filePath string) error {

	existingData, err := os.ReadFile(filePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	var existingMetrics *GameOSMetrics
	if len(existingData) > 0 {
		existingMetrics, err = DeserializeGameOSMetrics(existingData)
		if err != nil {
			return err
		}
	} else {
		existingMetrics = NewGameOSMetrics()
	}

	existingMetrics.Merge(g)

	data := SerializeGameOSMetrics(g)
	return os.WriteFile(filePath, data, filePermission)
}

// Returns the serialized representation of the GameOSMetrics
func SerializeGameOSMetrics(gameOsMetrics *GameOSMetrics) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], gameOsMetrics.Linux)
	binary.BigEndian.PutUint32(buf[4:8], gameOsMetrics.Windows)
	binary.BigEndian.PutUint32(buf[8:12], gameOsMetrics.Mac)
	return buf
}

func DeserializeGameOSMetrics(data []byte) (*GameOSMetrics, error) {
	if len(data) != 12 {
		return nil, errors.New("invalid data length")
	}

	linux := binary.BigEndian.Uint32(data[0:4])
	windows := binary.BigEndian.Uint32(data[4:8])
	mac := binary.BigEndian.Uint32(data[8:12])

	return &GameOSMetrics{
		Linux:   linux,
		Windows: windows,
		Mac:     mac,
	}, nil
}

func (g *GameOSMetrics) Merge(other *GameOSMetrics) {
	g.Linux += other.Linux
	g.Windows += other.Windows
	g.Mac += other.Mac
}

func (g *GameOSMetrics) AddGameOS(gameOS *GameOS) {
	if gameOS.Linux {
		g.Linux++
	}
	if gameOS.Windows {
		g.Windows++
	}
	if gameOS.Mac {
		g.Mac++
	}
}

func GetStrRepresentation(gameOs *GameOSMetrics) string {
	return "Linux: " + strconv.Itoa(int(gameOs.Linux)) + "\n" + "Windows: " + strconv.Itoa(int(gameOs.Windows)) + "\n" + "Mac: " + strconv.Itoa(int(gameOs.Mac)) + "\n"
}
