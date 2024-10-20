package system_protocol

import "errors"

type Message struct {
	MessageType MessageType
	ClientID    int
	Body        []byte
}

func DeserializeMessage(message []byte) (*Message, error) {
	if len(message) < 2 {
		return nil, errors.New("message too short to contain message header")
	}

	messageType := MessageType(message[0])
	clientID := int(message[1])

	return &Message{
		MessageType: messageType,
		ClientID:    clientID,
		Body:        message[2:],
	}, nil
}

func SerializeMessage(messageType MessageType, clientID int, body []byte) []byte {
	message := make([]byte, 2+len(body))
	message[0] = byte(messageType)
	message[1] = byte(clientID)
	copy(message[2:], body)
	return message
}
