package main

import (
	"bufio"
	cp "distribuidos-tp/internal/client_protocol"
	"net"
	"os"

	"github.com/op/go-logging"
)

const SERVER_IP = "entrypoint:3000"
const GameFile = 1
const ReviewFile = 0

var log = logging.MustGetLogger("log")

func main() {

	conn, err := net.Dial("tcp", SERVER_IP)
	if err != nil {
		log.Errorf("Error connecting:", err)
		return
	}
	defer conn.Close()

	file, err := os.Open("./client_data/games_reduced_cleaned.csv")
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

	file, err = os.Open("./client_data/steam_reviews_reduced_cleaned.csv")
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
		serializedBatch, pendingBytes, eof, err := cp.SerializeBatch(scanner, pendingBytes, 9000, ReviewFile)
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

}
