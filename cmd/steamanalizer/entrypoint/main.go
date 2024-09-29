package main

import (
	cp "distribuidos-tp/internal/clientprotocol"
	"fmt"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":3000")
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server listening on port 3000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Receive the batch of serialized data
	data, err := cp.ReceiveGameBatch(conn)
	if err != nil {
		fmt.Println("Error receiving game batch:", err)
		return
	}

	lines, err := cp.DeserializeGameBatch(data)
	if err != nil {
		fmt.Println("Error deserializing game batch:", err)
		return
	}

	for _, line := range lines {
		fmt.Println(line)
	}
}
