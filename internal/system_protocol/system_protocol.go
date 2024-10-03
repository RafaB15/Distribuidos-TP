package system_protocol

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"encoding/binary"
	"errors"
	"fmt"
)

type MessageType byte

const (
	MessageEndOfFile MessageType = iota
	MessageGameOsInformation
	MessageAccumulatedGameOsInformation
)

func DeserializeMessageType(message []byte) (MessageType, []byte, error) {
	if len(message) == 0 {
		return 0, nil, fmt.Errorf("empty message")
	}

	msgType := MessageType(message[0])
	switch msgType {
	case MessageEndOfFile, MessageGameOsInformation, MessageAccumulatedGameOsInformation:
		return msgType, message[1:], nil
	default:
		return 0, nil, fmt.Errorf("unknown message type: %d", msgType)
	}
}

func SerializeMessageGameOsInformation(gameOSList []*oa.GameOS) []byte {
	count := len(gameOSList)
	message := make([]byte, 3+count*3)
	message[0] = byte(MessageGameOsInformation)
	binary.BigEndian.PutUint16(message[1:3], uint16(count))

	offset := 3
	for i, gameOS := range gameOSList {
		serializedGameOS := oa.SerializeGameOs(gameOS)
		copy(message[offset+i*3:], serializedGameOS)
	}

	return message
}

func DeserializeMessageGameOsInformation(message []byte) ([]*oa.GameOS, error) {
	if len(message) < 2 {
		return nil, errors.New("message too short to contain count")
	}

	count := binary.BigEndian.Uint16(message[:2])
	offset := 2

	expectedLength := int(count) * 3
	if len(message[offset:]) < expectedLength {
		return nil, errors.New("message length does not match expected count")
	}

	var gameOSList []*oa.GameOS
	for i := 0; i < int(count); i++ {
		start := offset + i*3
		end := start + 3
		gameOS, err := oa.DeserializeGameOS(message[start:end])
		if err != nil {
			return nil, err
		}
		gameOSList = append(gameOSList, gameOS)
	}

	return gameOSList, nil
}

func SerializeMessageAccumulatedGameOsInformacion(metrics *oa.GameOSMetrics) ([]byte, error) {
	message := make([]byte, 3+3)
	message[0] = byte(MessageAccumulatedGameOsInformation)
	serializedMetrics := oa.SerializeGameOsMetrics(metrics)
	copy(message[1:], serializedMetrics)
	return message, nil
}

func DeserializeMessageAccumulatedGameOsInformacion(message []byte) (*oa.GameOSMetrics, error) {
	if len(message) < 3 {
		return nil, errors.New("message too short to contain metrics")
	}

	metrics, err := oa.DeserializeGameOsMetrics(message[1:])
	if err != nil {
		return nil, err
	}

	return metrics, nil
}
