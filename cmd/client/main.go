package main

import (
	cp "distribuidos-tp/internal/clientprotocol"
	"fmt"
	"net"
)

const SERVER_IP = "entrypoint:3000"

func main() {
	conn, err := net.Dial("tcp", SERVER_IP)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()

	message_to_write := cp.GetMessageToSend()
	message_to_write += "\n"
	_, err = conn.Write([]byte(message_to_write))
	if err != nil {
		fmt.Println("Error sending data:", err)
		return
	}

	fmt.Println("Message sent")
}
