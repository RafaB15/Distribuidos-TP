package games

import (
	"encoding/binary"
	"errors"
)

type GameName struct {
	AppId uint32
	Name  string
}

func NewGameName(appId uint32, name string) *GameName {
	return &GameName{
		AppId: appId,
		Name:  name,
	}
}

func SerializeGameName(g *GameName) ([]byte, error) {
	nameLength := uint16(len(g.Name))
	totalLength := 4 + 2 + len(g.Name)

	data := make([]byte, totalLength)

	binary.BigEndian.PutUint32(data[0:4], g.AppId)

	binary.BigEndian.PutUint16(data[4:6], nameLength)

	copy(data[6:], g.Name)

	return data, nil
}

func DeserializeGameName(data []byte) (*GameName, error) {
	if len(data) < 6 {
		return nil, errors.New("data too short")
	}

	appId := binary.BigEndian.Uint32(data[0:4])

	nameLength := binary.BigEndian.Uint16(data[4:6])

	if len(data) < int(6+nameLength) {
		return nil, errors.New("data too short for name")
	}

	name := string(data[6 : 6+nameLength])

	return &GameName{
		AppId: appId,
		Name:  name,
	}, nil
}
