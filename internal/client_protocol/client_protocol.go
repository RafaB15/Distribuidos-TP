package client_protocol

import (
	"bufio"
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
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

func SerializeBatch(fileScanner *bufio.Scanner, pendingBytes []byte, maxBytes int, fileOrigin int) ([]byte, []byte, bool, error) {
	var serializedLines []byte
	var actualNumLines int = 0
	if pendingBytes != nil {
		serializedLines = append(serializedLines, pendingBytes...)
		actualNumLines += 1
	}
	var eof bool = false
	var eofFlag = byte(0)
	var currentPendingBytes []byte

	for {
		if !fileScanner.Scan() {
			eofFlag = 1
			eof = true
			break
		}
		line := fileScanner.Text()
		serializedLine := SerializeLine(line)
		if len(serializedLines)+len(serializedLine) > maxBytes {
			currentPendingBytes = serializedLine
			break
		}
		serializedLines = append(serializedLines, serializedLine...)
		actualNumLines++
	}

	if fileScanner.Err() != nil && fileScanner.Err() != io.EOF && eof {
		return nil, currentPendingBytes, false, fileScanner.Err()
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

	return result, currentPendingBytes, eof, nil
}

func ReceiveBatch(connection net.Conn) ([]byte, int, bool, error) {
	lenghtToReadBytes, err := u.ReadExact(connection, LineLengthBytesAmount)

	if err != nil {
		return nil, 0, false, err
	}
	totalLength := binary.BigEndian.Uint32(lenghtToReadBytes)
	// log.Infof("Length to read: %v", totalLength)

	data, err := u.ReadExact(connection, int(totalLength))
	if err != nil {
		log.Errorf("Error reading data: %v", err)
		return nil, 0, false, err
	}

	fileType := int(data[0])
	eofFlag := data[1]

	// log.Infof("File type: %d", fileType)
	// log.Infof("EOF flag: %d", eofFlag)

	return data[2:], fileType, eofFlag == 1, nil
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
