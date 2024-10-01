package osaccumulator

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"strconv"
	"strings"
)

func ProcessCSV(input string) ([]byte, error) {
	reader := csv.NewReader(strings.NewReader(input))
	records, err := reader.Read()
	if err != nil {
		return nil, err
	}

	if len(records) < 20 {
		return nil, errors.New("input CSV does not have enough fields")
	}

	appID, err := strconv.ParseUint(records[0], 10, 64)
	if err != nil {
		return nil, err
	}

	bool17 := parseBool(records[17])
	bool18 := parseBool(records[18])
	bool19 := parseBool(records[19])

	result := make([]byte, 0, 11)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appID)
	result = append(result, buf...)
	result = append(result, bool17, bool18, bool19)

	return result, nil
}

func DeserializeGameOS(data []byte) (*GameOS, error) {
	if len(data) != 11 {
		return nil, errors.New("invalid data length")
	}

	appID := binary.BigEndian.Uint64(data[:8])
	linux := data[8] == 1
	windows := data[9] == 1
	mac := data[10] == 1

	return &GameOS{
		AppId:   appID,
		Linux:   linux,
		Windows: windows,
		Mac:     mac,
	}, nil
}

func parseBool(value string) byte {
	if value == "true" {
		return 1
	}
	return 0
}
