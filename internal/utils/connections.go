package utils

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

func WriteExact(writer io.Writer, data []byte) error {
	sentBytes := 0
	for sentBytes < len(data) {
		n, err := writer.Write(data[sentBytes:])
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("writer closed before writing expected amount of data")
		}
		sentBytes += n
	}
	return nil
}

func ReadExact(reader io.Reader, length int) ([]byte, error) {
	data := make([]byte, length)
	readBytes := 0

	for readBytes < length {
		n, err := reader.Read(data[readBytes:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errors.New("reader closed before reading expected amount of data")
		}
		readBytes += n
	}

	return data, nil
}

func HandlePing() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// fmt.Fprintln(w, "Pong")
	})

	if err := http.ListenAndServe(":80", nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}
