package system_protocol

import "errors"

type QueryResultMessage struct {
	Type     Query
	ClientID int
	Body     []byte
}

func DeserializeQuery(message []byte) (*QueryResultMessage, error) {
	if len(message) < 2 {
		return nil, errors.New("message too short to contain message header")
	}

	queryType := Query(message[0])
	clientID := int(message[1])

	return &QueryResultMessage{
		Type:     queryType,
		ClientID: clientID,
		Body:     message[2:],
	}, nil
}

func SerializeQuery(queryType Query, clientID int, body []byte) []byte {
	message := make([]byte, 2+len(body))
	message[0] = byte(queryType)
	message[1] = byte(clientID)
	copy(message[2:], body)
	return message
}
