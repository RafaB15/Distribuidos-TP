package topic_filters

import "encoding/binary"

type ActionGame struct {
	AppId    uint32
	GameName string
	Topic    string
}

func NewActionGame(appId uint32, gameName string) *ActionGame {
	return &ActionGame{
		AppId:    appId,
		GameName: gameName,
		Topic:    "Action",
	}
}

func SerializeActionGame(game *ActionGame) ([]byte, error) {
	totalLen := 4 + 4 + len(game.GameName) + 4 + len(game.Topic)
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], game.AppId)

	gameNameLen := uint32(len(game.GameName))
	binary.BigEndian.PutUint32(buf[4:8], gameNameLen)
	copy(buf[8:8+gameNameLen], []byte(game.GameName))

	topicStart := 8 + gameNameLen
	topicLen := uint32(len(game.Topic))
	binary.BigEndian.PutUint32(buf[topicStart:topicStart+4], topicLen)
	copy(buf[topicStart+4:topicStart+4+topicLen], []byte(game.Topic))

	return buf, nil
}

func DeserializeActionGame(data []byte) (*ActionGame, error) {

	appId := binary.BigEndian.Uint32(data[0:4])

	gameNameLen := binary.BigEndian.Uint32(data[4:8])
	gameName := string(data[8 : 8+gameNameLen])

	topicStart := 8 + gameNameLen
	topicLen := binary.BigEndian.Uint32(data[topicStart : topicStart+4])
	topic := string(data[topicStart+4 : topicStart+4+topicLen])

	return &ActionGame{
		AppId:    appId,
		GameName: gameName,
		Topic:    topic,
	}, nil
}
