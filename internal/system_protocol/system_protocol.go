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
)

type MessageType byte

const (
	MsgEndOfFile MessageType = iota
	MsgGameOSInformation
	MsgAccumulatedGameOSInformation
	MsgGameYearAndAvgPtfInformation
	MsgBatch
	MsgReviewInformation
	MsgQueryResolved
	MsgGameReviewsMetrics
	MsgGameNames
	MsgJoinedPositiveGameReviews
	MsgJoinedNegativeGameReviews
	MsgRawReviewInformation
	MsgRawReviewInformationBatch
)

// SerializeMsgEndOfFile Message End of file
func SerializeMsgEndOfFile(clientId int) []byte {
	return SerializeMessage(MsgEndOfFile, clientId, nil)
}

// --------------------------------------------------------

// SerializeMsgBatch Message Batch
func SerializeMsgBatch(clientId int, data []byte) []byte {
	return SerializeMessage(MsgBatch, clientId, data)
}

func DeserializeMsgBatch(data []byte) ([]string, error) {
	if len(data) == 0 {
		return []string{}, nil
	}

	numLines := int(binary.BigEndian.Uint32(data[:4]))

	serializedLines := data[4:]
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
	lineLengthBytesAmount := 4

	if offset+lineLengthBytesAmount > len(data) {
		return "", 0, errors.New("data too short to contain line length information")
	}

	lineLength := binary.BigEndian.Uint32(data[offset : offset+lineLengthBytesAmount])
	if int(lineLength) > len(data)-offset-lineLengthBytesAmount {
		return "", 0, errors.New("invalid line length information")
	}

	line := string(data[offset+lineLengthBytesAmount : offset+lineLengthBytesAmount+int(lineLength)])
	newOffset := offset + lineLengthBytesAmount + int(lineLength)

	return line, newOffset, nil
}

// --------------------------------------------------------

// SerializeMsgGameOSInformation Game Os Message
func SerializeMsgGameOSInformation(clientID int, gameOSList []*oa.GameOS) []byte {

	GameOSSize := 3     // Tamaño en bytes de un GameOS serializado
	CountFieldSize := 2 // Tamaño del campo que guarda el conteo

	count := len(gameOSList)

	// Crear el cuerpo del mensaje con el tamaño adecuado
	body := make([]byte, CountFieldSize+count*GameOSSize)            // Reservar espacio para el conteo y los GameOS serializados
	binary.BigEndian.PutUint16(body[:CountFieldSize], uint16(count)) // Header con la cantidad de elementos

	offset := CountFieldSize
	for i, gameOS := range gameOSList {
		serializedGameOS := oa.SerializeGameOS(gameOS)
		copy(body[offset+i*GameOSSize:], serializedGameOS) // Copiar cada GameOS serializado
	}

	return SerializeMessage(MsgGameOSInformation, clientID, body)
}

// DeserializeMsgGameOSInformation takes a byte slice representing a message and
// returns a slice of GameOS objects or an error if deserialization fails.
func DeserializeMsgGameOSInformation(message []byte) ([]*oa.GameOS, error) {

	GameOSSize := 3     // Tamaño en bytes de un GameOS serializado
	CountFieldSize := 2 // Tamaño del campo que guarda el conteo

	// Extract the number of GameOS records (count) from the first 2 bytes of the body
	count := binary.BigEndian.Uint16(message[:CountFieldSize])
	offset := CountFieldSize // Start reading the GameOS records after the 3-byte header

	// Calculate the expected length based on the number of GameOS records
	expectedLength := int(count) * GameOSSize

	// Ensure the message contains enough bytes to match the expected length of GameOS records
	if len(message[offset:]) < expectedLength {
		return nil, errors.New("message length does not match expected count")
	}

	var gameOSList []*oa.GameOS

	// Loop through the message to deserialize each GameOS record
	for i := 0; i < int(count); i++ {
		start := offset + i*GameOSSize
		end := start + GameOSSize

		// Deserialize the current GameOS record from the message slice
		gameOS, err := oa.DeserializeGameOS(message[start:end])
		if err != nil {
			return nil, err // Return an error if deserialization of a GameOS record fails
		}

		gameOSList = append(gameOSList, gameOS)
	}

	return gameOSList, nil
}

// --------------------------------------------------------
// Message RawReviewInformation

func SerializeMsgRawReviewInformation(clientID int, review *r.RawReview) []byte {
	serializedReview := review.Serialize()
	return SerializeMessage(MsgRawReviewInformation, clientID, serializedReview)
}

func DeserializeMsgRawReviewInformation(message []byte) (*r.RawReview, error) {
	rawReview, _, err := r.DeserializeRawReview(message)
	return rawReview, err
}

// --------------------------------------------------------
// Message RawReviewInformationBatch

func SerializeMsgRawReviewInformationBatch(clientID int, reviews []*r.RawReview) []byte {
	serializedReviews := r.SerializeRawReviewsBatch(reviews)
	return SerializeMessage(MsgRawReviewInformationBatch, clientID, serializedReviews)
}

func DeserializeMsgRawReviewInformationBatch(message []byte) ([]*r.RawReview, error) {
	return r.DeserializeRawReviewsBatch(message)
}

// --------------------------------------------------------
// Message GameYearAndAvgPtfInformation

func SerializeMsgGameYearAndAvgPtf(clientId int, gameYearAndAvgPtf []*df.GameYearAndAvgPtf) []byte {
	gameYearAndAvgPtfSize := 10
	amountSize := 2

	count := len(gameYearAndAvgPtf)
	message := make([]byte, amountSize+count*gameYearAndAvgPtfSize)
	binary.BigEndian.PutUint16(message[:amountSize], uint16(count))

	offset := amountSize
	for i, game := range gameYearAndAvgPtf {
		serializedGame := df.SerializeGameYearAndAvgPtf(game)
		copy(message[offset+i*gameYearAndAvgPtfSize:], serializedGame)
	}

	return SerializeMessage(MsgGameYearAndAvgPtfInformation, clientId, message)
}

func DeserializeMsgGameYearAndAvgPtf(message []byte) ([]*df.GameYearAndAvgPtf, error) {
	gameYearAndAvgPtfSize := 10
	amountSize := 2

	if len(message) < amountSize {
		return nil, errors.New("message too short to contain count")
	}

	count := binary.BigEndian.Uint16(message[:amountSize])
	offset := amountSize

	expectedLength := int(count) * gameYearAndAvgPtfSize
	if len(message[offset:]) < expectedLength {
		return nil, errors.New("message length does not match expected count")
	}

	var gameYearAndAvgPtfList []*df.GameYearAndAvgPtf
	for i := 0; i < int(count); i++ {
		start := offset + i*gameYearAndAvgPtfSize
		end := start + gameYearAndAvgPtfSize
		game, err := df.DeserializeGameYearAndAvgPtf(message[start:end])
		if err != nil {
			return nil, err
		}
		gameYearAndAvgPtfList = append(gameYearAndAvgPtfList, game)
	}

	return gameYearAndAvgPtfList, nil
}

// --------------------------------------------------------

// SerializeMsgReviewInformation Message Review Information
func SerializeMsgReviewInformation(clientID int, review *r.Review) []byte {
	serializedReview := review.Serialize()
	return SerializeMessage(MsgReviewInformation, clientID, serializedReview)
}

func DeserializeMsgReviewInformation(message []byte) (*r.Review, error) {
	return r.DeserializeReview(message)
}

// --------------------------------------------------------

// Message Game Reviews Metrics Batch

func SerializeMsgGameReviewsMetricsBatch(clientID int, metrics []*m.GameReviewsMetrics) []byte {
	gameReviewsMetricsSize := 12
	amountSize := 2

	count := len(metrics)
	body := make([]byte, amountSize+count*gameReviewsMetricsSize)
	binary.BigEndian.PutUint16(body[:amountSize], uint16(count))

	offset := amountSize
	for i, metric := range metrics {
		serializedMetrics := m.SerializeGameReviewsMetrics(metric)
		copy(body[offset+i*gameReviewsMetricsSize:], serializedMetrics)
	}

	return SerializeMessage(MsgGameReviewsMetrics, clientID, body)
}

func DeserializeMsgGameReviewsMetricsBatch(message []byte) ([]*m.GameReviewsMetrics, error) {
	gameReviewsMetricsSize := 12
	amountSize := 2

	if len(message) < amountSize {
		return nil, errors.New("message too short to contain count")
	}

	count := int(binary.BigEndian.Uint16(message[:amountSize]))
	offset := amountSize
	metrics := make([]*m.GameReviewsMetrics, count)

	for i := 0; i < count; i++ {
		if offset+gameReviewsMetricsSize > len(message) {
			return nil, errors.New("message too short to contain all metrics")
		}
		metric, err := m.DeserializeGameReviewsMetrics(message[offset : offset+gameReviewsMetricsSize])
		if err != nil {
			return nil, err
		}
		metrics[i] = metric
		offset += gameReviewsMetricsSize
	}

	return metrics, nil
}

// --------------------------------------------------------
// Message Game Names

func SerializeMsgGameNames(clientID int, gameNames []*g.GameName) ([]byte, error) {
	count := len(gameNames)
	headerSize := 2 // 2 bytes for count
	body := make([]byte, headerSize)

	binary.BigEndian.PutUint16(body[:headerSize], uint16(count))

	offset := headerSize
	for _, gameName := range gameNames {
		serializedGameName, err := g.SerializeGameName(gameName)
		if err != nil {
			return nil, err
		}
		body = append(body, serializedGameName...)
		offset += len(serializedGameName)
	}

	return SerializeMessage(MsgGameNames, clientID, body), nil
}

func DeserializeMsgGameNames(message []byte) ([]*g.GameName, error) {
	amountSize := 2

	if len(message) < amountSize {
		return nil, errors.New("message too short to contain count")
	}

	count := binary.BigEndian.Uint16(message[:amountSize])
	offset := amountSize

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

// --------------------------------------------------------
// Message Joined Positive Action Game Reviews

func SerializeMsgJoinedPositiveGameReviews(clientID int, joinedActionGameReview *j.JoinedPositiveGameReview) ([]byte, error) {
	messageLen := 4 + 4 + len(joinedActionGameReview.GameName) + 4
	message := make([]byte, messageLen) //chequear cuando haga el mensaje de ActionGame

	serializedJoinedPositiveGameReview, err := j.SerializeJoinedPositiveGameReview(joinedActionGameReview)
	if err != nil {
		return nil, err
	}
	copy(message, serializedJoinedPositiveGameReview)

	return SerializeMessage(MsgJoinedPositiveGameReviews, clientID, message), nil
}

func DeserializeMsgJoinedPositiveGameReviews(data []byte) (*j.JoinedPositiveGameReview, error) {

	metrics, err := j.DeserializeJoinedPositiveGameReview(data)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

// --------------------------------------------------------

// Message Joined Negative Action Game Reviews

func SerializeMsgJoinedNegativeGameReviews(clientID int, joinedActionNegativeGameReview *j.JoinedNegativeGameReview) ([]byte, error) {
	messageLen := 4 + 4 + len(joinedActionNegativeGameReview.GameName) + 4
	message := make([]byte, messageLen) //chequear cuando haga el mensaje de ActionGame

	serializedJoinedNegativeGameReview, err := j.SerializeJoinedActionNegativeGameReview(joinedActionNegativeGameReview)
	if err != nil {
		return nil, err
	}
	copy(message, serializedJoinedNegativeGameReview)

	return SerializeMessage(MsgJoinedNegativeGameReviews, clientID, message), nil
}

func DeserializeMsgJoinedNegativeGameReviews(data []byte) (*j.JoinedNegativeGameReview, error) {

	metrics, err := j.DeserializeJoinedActionNegativeGameReview(data)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

// --------------------------------------------------------

// Message Game Os Metrics

func SerializeGameOSMetrics(clientID int, gameMetrics *oa.GameOSMetrics) []byte {
	body := oa.SerializeGameOSMetrics(gameMetrics)
	return SerializeMessage(MsgAccumulatedGameOSInformation, clientID, body)
}

func DeserializeMsgAccumulatedGameOSInformationV2(message []byte) (*oa.GameOSMetrics, error) {
	return oa.DeserializeGameOSMetrics(message)
}

// --------------------------------------------------------
// Message Final Query Results

func AssembleFinalQueryMsg(clientID byte, messageType byte, body []byte) []byte {
	length := len(body)
	msg := make([]byte, 1+1+2+length)
	msg[0] = clientID
	msg[1] = messageType
	binary.BigEndian.PutUint16(msg[2:4], uint16(length))
	copy(msg[4:], body)

	return msg
}
