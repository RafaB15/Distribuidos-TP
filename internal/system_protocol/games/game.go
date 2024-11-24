package games

import (
	"encoding/binary"
	"errors"
)

const (
	AppIdLengthBytes = 4
	LengthBytes      = 2
	ActionByte       = 1
	IndieByte        = 1
)

type Game struct {
	AppId  uint32
	Name   string
	Action bool
	Indie  bool
}

func NewGame(appId uint32, name string, action bool, indie bool) *Game {
	return &Game{
		AppId:  appId,
		Name:   name,
		Action: action,
		Indie:  indie,
	}
}

func SerializeGame(g *Game) ([]byte, error) {
	nameLength := uint16(len(g.Name))
	totalLength := AppIdLengthBytes + LengthBytes + len(g.Name) + ActionByte + IndieByte

	data := make([]byte, totalLength)
	offset := 0

	binary.BigEndian.PutUint32(data[offset:offset+AppIdLengthBytes], g.AppId)
	offset += AppIdLengthBytes

	binary.BigEndian.PutUint16(data[offset:offset+LengthBytes], nameLength)
	offset += LengthBytes

	copy(data[offset:offset+int(nameLength)], g.Name)
	offset += int(nameLength)

	if g.Action {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset++

	if g.Indie {
		data[offset] = 1
	} else {
		data[offset] = 0
	}

	return data, nil
}

func DeserializeGame(data []byte) (game *Game, amountRead int, e error) {
	if len(data) < AppIdLengthBytes+LengthBytes+ActionByte+IndieByte {
		return nil, 0, errors.New("data too short")
	}

	offset := 0

	appId := binary.BigEndian.Uint32(data[offset : offset+AppIdLengthBytes])
	offset += AppIdLengthBytes

	nameLength := binary.BigEndian.Uint16(data[offset : offset+LengthBytes])
	offset += LengthBytes

	if len(data[offset:]) < int(nameLength) {
		return nil, 0, errors.New("data too short for name")
	}

	name := string(data[offset : offset+int(nameLength)])
	offset += int(nameLength)

	action := data[offset] == 1
	offset++

	indie := data[offset] == 1
	offset++

	return &Game{
		AppId:  appId,
		Name:   name,
		Action: action,
		Indie:  indie,
	}, offset, nil
}

func SerializeGameBatch(games []*Game) ([]byte, error) {
	var result []byte

	gameCount := uint16(len(games))
	countBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(countBytes, gameCount)
	result = append(result, countBytes...)

	for _, game := range games {
		serializedGame, err := SerializeGame(game)
		if err != nil {
			return nil, err
		}
		result = append(result, serializedGame...)
	}

	return result, nil
}

func DeserializeGameBatch(data []byte) ([]*Game, error) {
	if len(data) < 2 {
		return nil, errors.New("data too short")
	}

	offset := 0

	gameCount := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	games := make([]*Game, 0, gameCount)

	for i := 0; i < int(gameCount); i++ {
		game, n, err := DeserializeGame(data[offset:])
		if err != nil {
			return nil, err
		}
		offset += n
		games = append(games, game)
	}

	return games, nil
}
