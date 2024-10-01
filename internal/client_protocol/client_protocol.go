package client_protocol

import (
	"bufio"
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

const LineLengthBytesAmount = 4
const LinesNumberBytesAmount = 1

func SerializeGame(line string) []byte {
	var result []byte

	lineBytes := []byte(line)
	lineLength := len(lineBytes)

	lineLengthBytes := make([]byte, LineLengthBytesAmount)
	binary.BigEndian.PutUint32(lineLengthBytes, uint32(lineLength))
	result = append(result, lineLengthBytes...)
	result = append(result, lineBytes...)

	return result
}

func SerializeGameBatch(fileScanner *bufio.Scanner, numLines int) ([]byte, error) {
	var serializedLines []byte
	var actualNumLines int = 0

	for i := 0; i < numLines && fileScanner.Scan(); i++ {
		line := fileScanner.Text()
		serializedLine := SerializeGame(line)
		serializedLines = append(serializedLines, serializedLine...)
		actualNumLines += 1
	}

	if err := fileScanner.Err(); err != nil {
		return nil, err
	}

	result := make([]byte, 0, len(serializedLines)+LineLengthBytesAmount+LinesNumberBytesAmount)

	totalLengthBytes := make([]byte, LineLengthBytesAmount)
	binary.BigEndian.PutUint32(totalLengthBytes, uint32(len(serializedLines)+LinesNumberBytesAmount))
	result = append(result, totalLengthBytes...)

	numLinesBytes := make([]byte, LinesNumberBytesAmount)
	numLinesBytes[0] = byte(actualNumLines)
	result = append(result, numLinesBytes...)

	result = append(result, serializedLines...)

	return result, nil
}

func ReceiveGameBatch(connection net.Conn) ([]byte, error) {
	lenghtToReadBytes, err := u.ReadExact(connection, LineLengthBytesAmount)
	if err != nil {
		return nil, err
	}

	totalLength := binary.BigEndian.Uint32(lenghtToReadBytes)
	data, err := u.ReadExact(connection, int(totalLength))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func DeserializeGameBatch(data []byte) ([]string, error) {
	if len(data) < 1 {
		return nil, errors.New("data too short to contain number of lines")
	}

	numLines := int(data[0])
	fmt.Printf("Numlines: %d\n", numLines)
	serializedLines := data[1:]

	var lines []string
	offset := 0

	for i := 0; i < numLines; i++ {
		line, newOffset, err := DeserializeGameLine(serializedLines, offset)
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
		offset = newOffset
	}

	return lines, nil
}

func DeserializeGameLine(data []byte, offset int) (string, int, error) {
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
