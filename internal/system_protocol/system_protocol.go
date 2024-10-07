package system_protocol

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	r "distribuidos-tp/internal/system_protocol/reviews"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/op/go-logging"
)

type MessageType byte

var log = logging.MustGetLogger("log")

const (
	MsgEndOfFile MessageType = iota
	MsgGameOSInformation
	MsgAccumulatedGameOSInformation
	MsgGameYearAndAvgPtfInformation
	MsgBatch
	MsgReviewInformation
	MsgQueryResolved
)

// Size of the bytes to store the length of the payload
const LineLengthBytesAmount = 4

// Size of the bytes to store the number of lines in the payload
const LinesNumberBytesAmount = 1

// Size of the bytes to store the origin of the file
const FileOriginBytesAmount = 1

func DeserializeMessageType(message []byte) (MessageType, error) {
	if len(message) == 0 {
		return 0, fmt.Errorf("empty message")
	}

	return MessageType(message[0]), nil

}

func SerializeBatchMsg(batch []byte) []byte {
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

func SerializeMsgGameYearAndAvgPtf(gameYearAndAvgPtf []*df.GameYearAndAvgPtf) []byte {
	count := len(gameYearAndAvgPtf)
	message := make([]byte, 3+count*10)
	message[0] = byte(MsgGameYearAndAvgPtfInformation)
	binary.BigEndian.PutUint16(message[1:3], uint16(count))

	offset := 3
	for i, game := range gameYearAndAvgPtf {
		serializedGame := df.SerializeGameYearAndAvgPtf(game)
		copy(message[offset+i*10:], serializedGame)
	}

	return message
}

func DeserializeMsgGameYearAndAvgPtf(message []byte) ([]*df.GameYearAndAvgPtf, error) {
	if len(message) < 3 {
		return nil, errors.New("message too short to contain count")
	}

	count := binary.BigEndian.Uint16(message[1:3])
	offset := 3

	expectedLength := int(count) * 10
	if len(message[offset:]) < expectedLength {
		return nil, errors.New("message length does not match expected count")
	}

	var gameYearAndAvgPtfList []*df.GameYearAndAvgPtf
	for i := 0; i < int(count); i++ {
		start := offset + i*10
		end := start + 10
		game, err := df.DeserializeGameYearAndAvgPtf(message[start:end])
		if err != nil {
			return nil, err
		}
		gameYearAndAvgPtfList = append(gameYearAndAvgPtfList, game)
	}

	return gameYearAndAvgPtfList, nil
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

func SerializeMsgAccumulatedGameOSInfo(data []byte) ([]byte, error) {
	message := make([]byte, 1+12)
	message[0] = byte(MsgAccumulatedGameOSInformation)
	copy(message[1:], data)
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

func DeserializeMsgReviewInformation(message []byte) ([]*r.Review, error) {
	if len(message) < 3 {
		return nil, errors.New("message too short to contain count")
	}

	count := binary.BigEndian.Uint16(message[1:3])
	offset := 3

	expectedLength := int(count) * 5
	if len(message[offset:]) < expectedLength {
		return nil, errors.New("message length does not match expected count")
	}

	var reviews []*r.Review
	for i := 0; i < int(count); i++ {
		start := offset + i*5
		end := start + 5
		review, err := r.DeserializeReview(message[start:end])
		if err != nil {
			return nil, err
		}
		reviews = append(reviews, review)
	}

	return reviews, nil
}

func DeserializeBatch(data []byte) ([]string, error) {

	if len(data) == 0 {
		return []string{}, nil
	}

	numLines := int(data[1])

	serializedLines := data[2:]
	var lines []string

	offset := 0

	for i := 0; i < numLines; i++ {
		line, newOffset, _ := DeserializeLine(serializedLines, offset)
		lines = append(lines, line)
		log.Infof("Deserialized game line: %s", line)
		offset = newOffset
	}

	return lines, nil
}

func DeserializeLine(data []byte, offset int) (string, int, error) {
	if offset+LineLengthBytesAmount > len(data) {
		return "", 0, errors.New("data too short to contain line length information")
	}

	lineLength := binary.BigEndian.Uint32(data[offset : offset+LineLengthBytesAmount])
	if int(lineLength) > len(data)-offset-LineLengthBytesAmount {
		return "", 0, errors.New("invalid line length information")
	}

	line := string(data[offset+LineLengthBytesAmount : offset+LineLengthBytesAmount+int(lineLength)])
	newOffset := offset + LineLengthBytesAmount + int(lineLength)

	return line, newOffset, nil
}
