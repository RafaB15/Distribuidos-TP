package utils

import (
	"errors"
	"os"
)

func ReadExactFromFile(file *os.File, length int) ([]byte, error) {
	data := make([]byte, length)
	readBytes := 0

	for readBytes < length {
		n, err := file.Read(data[readBytes:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errors.New("file closed before reading expected amount of data")
		}
		readBytes += n
	}

	return data, nil
}

func WriteAllToFile(file *os.File, data []byte) error {
	totalWritten := 0
	dataLength := len(data)

	for totalWritten < dataLength {
		n, err := file.Write(data[totalWritten:])
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("file closed before writing all data")
		}
		totalWritten += n
	}

	return nil
}
