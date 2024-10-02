package os_accumulator

import (
	"encoding/binary"
	"errors"
)

func SerializeGameOs(gameOs *GameOS) []byte {
	result := make([]byte, 0, 11)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, gameOs.AppId)
	result = append(result, buf...)
	result = append(result, parseBoolByte(gameOs.Linux), parseBoolByte(gameOs.Linux), parseBoolByte(gameOs.Linux))

	return result
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

func parseBoolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}
