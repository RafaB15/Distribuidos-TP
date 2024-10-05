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

	for {
		serializedBatch, eof, err := cp.SerializeBatch(scanner, 10, GameFile)
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
		if eof {
			log.Debug("End of file")
			break
		}
		log.Debug("Sent game batch")
	}

}
