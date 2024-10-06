package system_protocol

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"
	"encoding/binary"
	"errors"
	"fmt"
)

type MessageType byte

const (
	MsgEndOfFile MessageType = iota
	MsgGameOSInformation
	MsgAccumulatedGameOSInformation
	MsgBatch
	MsgReviewInformation
)

func DeserializeMessageType(message []byte) (MessageType, error) {
	if len(message) == 0 {
		return 0, fmt.Errorf("empty message")
	}

	msgType := MessageType(message[0])
	switch msgType {
	case MsgEndOfFile, MsgGameOSInformation, MsgAccumulatedGameOSInformation, MsgBatch:
		return msgType, nil
	default:
		return 0, fmt.Errorf("unknown message type: %d", msgType)
	}
}

func SerializeBatchMsg(batch string) []byte {
	message := make([]byte, 1+len(batch))
	message[0] = byte(MsgBatch)
	copy(message[1:], batch)
	return message
}

func DeserializeBatchMsg(message []byte) (string, error) {
	if len(message) == 0 {
		return "", errors.New("empty message")
	}

	return string(message[1:]), nil
}

func SerializeMsgGameOSInformation(gameOSList []*oa.GameOS) []byte {
	count := len(gameOSList)
	message := make([]byte, 3+count*3)
	message[0] = byte(MsgGameOSInformation)
	binary.BigEndian.PutUint16(message[1:3], uint16(count))

	offset := 3
	for i, gameOS := range gameOSList {
		serializedGameOS := oa.SerializeGameOS(gameOS)
		copy(message[offset+i*3:], serializedGameOS)
	}

	return message
}

func DeserializeMsgGameOSInformation(message []byte) ([]*oa.GameOS, error) {
	if len(message) < 3 {
		return nil, errors.New("message too short to contain count")
	}

	count := binary.BigEndian.Uint16(message[1:3])
	offset := 3

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

func SerializeMsgAccumulatedGameOSInfo(metrics *oa.GameOSMetrics) ([]byte, error) {
	message := make([]byte, 1+12)
	message[0] = byte(MsgAccumulatedGameOSInformation)
	serializedMetrics := oa.SerializeGameOSMetrics(metrics)
	copy(message[1:], serializedMetrics)
	return message, nil
}

func DeserializeMsgAccumulatedGameOSInformation(message []byte) (*oa.GameOSMetrics, error) {
	if len(message) < 13 {
		return nil, errors.New("message too short to contain metrics")
	}

	metrics, err := oa.DeserializeGameOSMetrics(message[1:])
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func SerializeMsgEndOfFile() []byte {
	return []byte{byte(MsgEndOfFile)}
}

func SerializeMsgReviewInformation(reviews []*r.Review) []byte {
	count := len(reviews)
	message := make([]byte, 3+count*5)
	message[0] = byte(MsgReviewInformation)
	binary.BigEndian.PutUint16(message[1:3], uint16(count))

	offset := 3
	for i, review := range reviews {
		serializedReview := review.Serialize()
		copy(message[offset+i*5:], serializedReview)
	}

	return message
}
