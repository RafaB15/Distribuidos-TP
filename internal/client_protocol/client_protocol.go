package client_protocol

import (
	"bufio"
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Size of the bytes to store the length of the payload
const LineLengthBytesAmount = 4

// Size of the bytes to store the number of lines in the payload
const LinesNumberBytesAmount = 1

// Size of the bytes to store the origin of the file
const FileOriginBytesAmount = 1

const EOFBytesAmount = 1

// File origin flags
const GameFile = 1
const ReviewFile = 0

func SerializeBatch(fileScanner *bufio.Scanner, numLines int, fileOrigin int) ([]byte, bool, error) {
	var serializedLines []byte
	var actualNumLines int = 0
	var eof bool = false
	var eofFlag = byte(0)

	for actualNumLines < numLines {
		if !fileScanner.Scan() {
			eofFlag = 1
			eof = true
			break
		}
		line := fileScanner.Text()
		serializedLine := SerializeLine(line)
		serializedLines = append(serializedLines, serializedLine...)
		actualNumLines++
	}

	if fileScanner.Err() != nil && fileScanner.Err() != io.EOF && eof {
		return nil, false, fileScanner.Err()
	}

	result := make([]byte, 0, len(serializedLines)+LineLengthBytesAmount+LinesNumberBytesAmount+FileOriginBytesAmount+EOFBytesAmount)
	totalLengthBytes := make([]byte, LineLengthBytesAmount)
	binary.BigEndian.PutUint32(totalLengthBytes, uint32(len(serializedLines)+LinesNumberBytesAmount+FileOriginBytesAmount+EOFBytesAmount))

	result = append(result, totalLengthBytes...)

	result = append(result, byte(fileOrigin))
	result = append(result, eofFlag)

	numLinesBytes := make([]byte, LinesNumberBytesAmount)
	numLinesBytes[0] = byte(actualNumLines)
	result = append(result, numLinesBytes...)

	result = append(result, serializedLines...)

	return result, eof, nil
}

func ReceiveBatch(connection net.Conn) ([]byte, int, bool, error) {
	lenghtToReadBytes, err := u.ReadExact(connection, LineLengthBytesAmount)

	if err != nil {
		return nil, 0, false, err
	}
	totalLength := binary.BigEndian.Uint32(lenghtToReadBytes)
	log.Infof("Length to read: %v", totalLength)

	data, err := u.ReadExact(connection, int(totalLength))
	if err != nil {
		log.Errorf("Error reading data: %v", err)
		return nil, 0, false, err
	}

	fileType := int(data[0])
	eofFlag := data[1]

	log.Infof("File type: %d", fileType)
	log.Infof("EOF flag: %d", eofFlag)

	return data[2:], fileType, eofFlag == 1, nil
}

func DeserializeBatch(data []byte) ([]string, error) {

	if len(data) == 0 {
		return []string{}, nil
	}

	numLines := int(data[0])

	serializedLines := data[1:]
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

func SerializeLine(line string) []byte {
	var result []byte

	lineBytes := []byte(line)
	lineLength := len(lineBytes)

	lineLengthBytes := make([]byte, LineLengthBytesAmount)
	binary.BigEndian.PutUint32(lineLengthBytes, uint32(lineLength))
	result = append(result, lineLengthBytes...)
	result = append(result, lineBytes...)

	return result
}
