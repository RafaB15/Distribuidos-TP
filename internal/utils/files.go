package utils

import (
	"errors"
	"os"
)

func TruncateAndWriteAllToFile(filename string, data []byte) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

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

func ReadAllFromFile(filename string) ([]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	data := make([]byte, fileSize)
	readBytes := 0

	for readBytes < int(fileSize) {
		n, err := file.Read(data[readBytes:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errors.New("file closed before reading all data")
		}
		readBytes += n
	}

	return data, nil
}
