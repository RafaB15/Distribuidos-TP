package system_protocol

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	m "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	g "distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"
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
	MsgFilteredYearAndAvgPtfInformation
	MsgBatch
	MsgReviewInformation
	MsgQueryResolved
	MsgGameReviewsMetrics
	MsgGameNames
	MsgIndiePositiveJoinedReviews
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

func SerializeMsgFilteredGameYearAndAvgPtf(gameYearAndAvgPtf []*df.GameYearAndAvgPtf) []byte {
	count := len(gameYearAndAvgPtf)
	message := make([]byte, 3+count*10)
	message[0] = byte(MsgFilteredYearAndAvgPtfInformation)
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

func SerializeMsgGameReviewsMetricsBatch(metrics []*m.GameReviewsMetrics) []byte {
	count := len(metrics)
	message := make([]byte, 3+count*12)
	message[0] = byte(MsgGameReviewsMetrics)
	binary.BigEndian.PutUint16(message[1:3], uint16(count))

	offset := 3
	for i, metric := range metrics {
		serializedMetrics := m.SerializeGameReviewsMetrics(metric)
		copy(message[offset+i*12:], serializedMetrics)
	}

	return message
}

func DeserializeMsgGameReviewsMetricsBatch(message []byte) ([]*m.GameReviewsMetrics, error) {
	if len(message) < 3 {
		return nil, errors.New("message too short to contain count")
	}

	count := int(binary.BigEndian.Uint16(message[1:3]))
	offset := 3
	metrics := make([]*m.GameReviewsMetrics, count)

	for i := 0; i < count; i++ {
		if offset+12 > len(message) {
			return nil, errors.New("message too short to contain all metrics")
		}
		metric, err := m.DeserializeGameReviewsMetrics(message[offset : offset+12])
		if err != nil {
			return nil, err
		}
		metrics[i] = metric
		offset += 12
	}

	return metrics, nil
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

func SerializeMsgJoinedActionGameReviews(joinedActionGameReview *j.JoinedActionGameReview) ([]byte, error) {
	messageLen := 4 + 4 + len(joinedActionGameReview.GameName) + 4
	message := make([]byte, 2+messageLen) //chequear cuando haga el mensaje de ActionGame
	message[0] = byte(MsgQueryResolved)
	message[1] = byte(MsgActionPositiveReviewsQuery)
	serializedJoinedActionGameReview, err := j.SerializeJoinedActionGameReview(joinedActionGameReview)
	if err != nil {
		return nil, err
	}
	copy(message[2:], serializedJoinedActionGameReview)
	return message, nil
}

func DeserializeMsgJoinedActionGameReviews(data []byte) (*j.JoinedActionGameReview, error) {

	metrics, err := j.DeserializeJoinedActionGameReview(data[2:]) //me salteo los 2 bytesde tipo de mensaje
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func SerializeMsgNegativeJoinedActionGameReviews(joinedActionNegativeGameReview *j.JoinedActionNegativeGameReview) ([]byte, error) {
	serializedJoinedActionNegativeGameReview, err := j.SerializeJoinedActionNegativeGameReview(joinedActionNegativeGameReview)
	message := make([]byte, 2+len(serializedJoinedActionNegativeGameReview)) //chequear cuando haga el mensaje de ActionGame
	message[0] = byte(MsgQueryResolved)
	message[1] = byte(MsgActionNegativeReviewsQuery)
	if err != nil {
		return nil, err
	}
	copy(message[2:], serializedJoinedActionNegativeGameReview)
	return message, nil
}

func DeserializeMsgNegativeJoinedActionGameReviews(data []byte) (*j.JoinedActionGameReview, error) {

	metrics, err := j.DeserializeJoinedActionGameReview(data[2:]) //me salteo los 2 bytesde tipo de mensaje
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func SerializeMsgJoinedIndieGameReviews(joinedActionGameReview *j.JoinedActionGameReview) ([]byte, error) {
	messageLen := 4 + 4 + len(joinedActionGameReview.GameName) + 4
	message := make([]byte, 2+messageLen) //chequear cuando haga el mensaje de ActionGame
	message[0] = byte(MsgQueryResolved)
	message[1] = byte(MsgIndiePositiveJoinedReviewsQuery)
	serializedJoinedActionGameReview, err := j.SerializeJoinedActionGameReview(joinedActionGameReview)
	if err != nil {
		return nil, err
	}
	copy(message[2:], serializedJoinedActionGameReview)
	return message, nil
}

func DeserializeMsgJoinedIndieGameReviews(data []byte) (*j.JoinedActionGameReview, error) {

	metrics, err := j.DeserializeJoinedActionGameReview(data[2:]) //me salteo los 2 bytesde tipo de mensaje
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func SerializeMsgJoinedIndieGameReviewsBatch(joinedActionGameReviews []*j.JoinedActionGameReview) []byte {
	count := len(joinedActionGameReviews)
	message := make([]byte, 3)
	message[0] = byte(MsgQueryResolved)
	message[1] = byte(MsgIndiePositiveJoinedReviewsQuery)
	message[2] = byte(count)

	offset := 3
	for _, joinedActionGameReview := range joinedActionGameReviews {
		serializedJoinedActionGameReview, err := j.SerializeJoinedActionGameReview(joinedActionGameReview)
		if err != nil {
			return nil
		}
		message = append(message, serializedJoinedActionGameReview...)
		offset += len(serializedJoinedActionGameReview)
	}

	return message
}

func DeserializeMsgJoinedIndieGameReviewsBatch(message []byte) ([]*j.JoinedActionGameReview, error) {
	// Función asume que nos viene sin el primer header
	if len(message) < 1 {
		return nil, errors.New("message too short to contain count")
	}

	count := int(message[0])
	offset := 1
	joinedActionGameReviews := make([]*j.JoinedActionGameReview, count)

	for i := 0; i < count; i++ {
		joinedActionGameReview, err := j.DeserializeJoinedActionGameReview(message[offset:])
		if err != nil {
			return nil, err
		}
		joinedActionGameReviews[i] = joinedActionGameReview
		offset += 4 + 2 + len([]byte(joinedActionGameReview.GameName)) + 4
	}

	return joinedActionGameReviews, nil
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
		offset = newOffset
	}

	return lines, nil
}

func SerializeMsgGameNames(gameNames []*g.GameName) ([]byte, error) {
	count := len(gameNames)
	headerSize := 3 // 1 byte for message type + 2 bytes for count
	message := make([]byte, headerSize)

	message[0] = byte(MsgGameNames)
	binary.BigEndian.PutUint16(message[1:3], uint16(count))

	offset := headerSize
	for _, gameName := range gameNames {
		serializedGameName, err := g.SerializeGameName(gameName)
		if err != nil {
			return nil, err
		}
		message = append(message, serializedGameName...)
		offset += len(serializedGameName)
	}

	return message, nil
}

func DeserializeMsgGameNames(message []byte) ([]*g.GameName, error) {
	if len(message) < 3 {
		return nil, errors.New("message too short to contain count")
	}

	count := binary.BigEndian.Uint16(message[1:3])
	offset := 3

	var gameNames []*g.GameName
	for i := 0; i < int(count); i++ {
		gameName, err := g.DeserializeGameName(message[offset:])
		if err != nil {
			return nil, err
		}
		gameNames = append(gameNames, gameName)
		offset += 6 + len(gameName.Name) // 4 bytes for AppId + 2 bytes for name length + name length
	}

	return gameNames, nil
}

// ----------------------------------------------------------
// ----------------------------------------------------------
// ----------------------------------------------------------
// ----------------------------------------------------------
// REFACTOR ZONE

// Message End of file
func SerializeMsgEndOfFileV2(clientId int) []byte {
	return SerializeMessage(MsgEndOfFile, clientId, nil)
}

// --------------------------------------------------------

// Message Batch
func SerializeMsgBatch(clientId int, data []byte) []byte {
	return SerializeMessage(MsgBatch, clientId, data)
}

func DeserializeMsgBatch(data []byte) ([]string, error) {
	if len(data) == 0 {
		return []string{}, nil
	}

	numLines := int(data[0])

	serializedLines := data[1:]
	var lines []string

	offset := 0

	for i := 0; i < numLines; i++ {
		line, newOffset, err := DeserializeLine(serializedLines, offset)
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
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
