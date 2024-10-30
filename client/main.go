package main

import (
	"bufio"
	cp "distribuidos-tp/internal/client_protocol"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/op/go-logging"
)

const SERVER_IP = "entrypoint:3000"
const GameFile = 1
const ReviewFile = 0

var log = logging.MustGetLogger("log")

func main() {

	gameFilePath := os.Getenv("GAME_FILE_PATH")
	if gameFilePath == "" {
		log.Errorf("GAME_FILE_PATH not set")
		return
	}
	reviewFilePath := os.Getenv("REVIEW_FILE_PATH")
	if reviewFilePath == "" {
		log.Errorf("REVIEW_FILE_PATH not set")
		return
	}

	conn, err := net.Dial("tcp", SERVER_IP)
	if err != nil {
		log.Errorf("Error connecting:", err)
		return
	}
	defer conn.Close()

	file, err := os.Open(gameFilePath)
	if err != nil {
		log.Errorf("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Skip the first line (headers)
	if scanner.Scan() {
		log.Debug("Skipped header line")
	}

	var pendingBytes []byte

	for {
		serializedBatch, pendingBytesFor, eof, err := cp.SerializeBatch(scanner, pendingBytes, 8000, GameFile)
		pendingBytes = pendingBytesFor
		if err != nil {
			log.Errorf("Error reading csv file: ", err)
			return
		}
		if serializedBatch != nil {
			_, err = conn.Write(serializedBatch)

			if err != nil {
				log.Errorf("Error sending game batch to entrypoint: ", err)
				return
			}
		}
		if eof && pendingBytes == nil {
			log.Debug("End of games file")
			break
		}
		log.Debug("Sent game batch")
	}

	file, err = os.Open(reviewFilePath)
	if err != nil {
		log.Errorf("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner = bufio.NewScanner(file)

	// Skip the first line (headers)
	if scanner.Scan() {
		log.Debug("Skipped header line")
	}

	pendingBytes = nil

	for {
		serializedBatch, pendingBytesFor, eof, err := cp.SerializeBatch(scanner, pendingBytes, 9000, ReviewFile)
		pendingBytes = pendingBytesFor
		if err != nil {
			log.Errorf("Error reading csv file: ", err)
			return
		}
		if serializedBatch != nil {
			_, err = conn.Write(serializedBatch)

			if err != nil {
				log.Errorf("Error sending review batch to entrypoint: ", err)
				return
			}
		}
		if eof && pendingBytes == nil {
			log.Debug("End of reviews file")
			break
		}
		log.Debug("Sent reviews batch")
	}

	for i := 0; i < 5; i++ {
		var clientNumber byte
		err := binary.Read(conn, binary.BigEndian, &clientNumber)
		if err != nil {
			log.Errorf("Error reading client number from connection: %v", err)
			return
		}
		log.Infof("Results for client %d:", clientNumber)

		fileName := fmt.Sprintf("/client_data/client_%d_results.txt", clientNumber)
		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Errorf("Error opening file: %v", err)
			return
		}
		defer file.Close()

		var queryNumber byte
		err = binary.Read(conn, binary.BigEndian, &queryNumber)
		if err != nil {
			log.Errorf("Error reading query number from connection: %v", err)
			return
		}
		log.Infof("Results for query %d:", queryNumber+1)

		var length uint16
		err = binary.Read(conn, binary.BigEndian, &length)
		if err != nil {
			log.Errorf("Error reading length from connection: %v", err)
			return
		}

		message := make([]byte, length)
		totalRead := 0
		for totalRead < int(length) {
			n, err := conn.Read(message[totalRead:])
			if err != nil {
				log.Errorf("Error reading message from connection: %v", err)
				return
			}
			totalRead += n
		}

		log.Infof("Received message: \n%s", string(message))

		_, err = file.WriteString(fmt.Sprintf("Query %d:\n%s\n", queryNumber+1, string(message)))
		if err != nil {
			log.Errorf("Error writing to file: %v", err)
			return
		}
	}
}
