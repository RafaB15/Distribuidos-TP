package joiner

import (
	"encoding/binary"
	"fmt"
)

type GameToSend struct {
	AppId      uint32
	Name       string
	ShouldSend bool
}

func NewGameToSend(appId uint32, name string, shouldSend bool) *GameToSend {
	return &GameToSend{
		AppId:      appId,
		Name:       name,
		ShouldSend: shouldSend,
	}
}

func SerializeGameToSend(game *GameToSend) []byte {
	buf := make([]byte, 7+len(game.Name))
	offset := 0

	binary.LittleEndian.PutUint32(buf[offset:offset+4], game.AppId)
	offset += 4

	nameLen := uint16(len(game.Name))
	binary.LittleEndian.PutUint16(buf[offset:offset+2], nameLen)
	offset += 2

	copy(buf[offset:offset+int(nameLen)], game.Name)
	offset += int(nameLen)

	if game.ShouldSend {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}

	return buf
}

func DeserializeGameToSend(data []byte) (*GameToSend, error) {
	offset := 0

	appId := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	nameLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	if len(data[offset:]) < int(nameLen) {
		return nil, fmt.Errorf("invalid data length: %d", len(data))
	}

	name := string(data[offset : offset+int(nameLen)])
	offset += int(nameLen)

	shouldSend := data[offset] == 1

	return &GameToSend{
		AppId:      appId,
		Name:       name,
		ShouldSend: shouldSend,
	}, nil
}
