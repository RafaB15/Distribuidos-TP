package main

import (
	"bufio"
	cp "distribuidos-tp/internal/client_protocol"
	"net"
	"os"

	"github.com/op/go-logging"
)

const SERVER_IP = "entrypoint:3000"

var log = logging.MustGetLogger("log")

func main() {
	conn, err := net.Dial("tcp", SERVER_IP)
	if err != nil {
		log.Errorf("Error connecting:", err)
		return
	}
	defer conn.Close()

	file, err := os.Open("./client_data/games.csv")
	if err != nil {
		log.Errorf("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		serializedBatch, err := cp.SerializeGameBatch(scanner, 10)
		if err != nil {
			log.Errorf("Error reading csv file: ", err)
			return
		}

		_, err = conn.Write(serializedBatch)

		if err != nil {
			log.Errorf("Error sending game batch to entrypoint: ", err)
			return
		}
		log.Debug("Sent game batch")
		break
	}
}
