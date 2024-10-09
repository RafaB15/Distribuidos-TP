package system_protocol

import "fmt"

type Query byte

const (
	MsgOsResolvedQuery Query = iota
	MsgActionPositiveReviewsQuery
)

func DeserializeQueryResolvedMsg(message []byte) (Query, error) {
	if len(message) == 0 {
		return 0, fmt.Errorf("empty message")
	}

	query := Query(message[0])
	switch query {
	case MsgOsResolvedQuery, MsgActionPositiveReviewsQuery:
		return query, nil
	default:
		return 0, fmt.Errorf("unknown message type: %d", query)
	}
}

func SerializeOsResolvedQueryMsg(data []byte) []byte {
	message := make([]byte, 2+len(data))
	message[0] = byte(MsgQueryResolved)
	message[1] = byte(MsgOsResolvedQuery)
	copy(message[2:], data)
	return message
}

func SerializeActionPositiveReviewsQueryMsg(data []byte) []byte {
	message := make([]byte, 2+len(data))
	message[0] = byte(MsgQueryResolved)
	message[1] = byte(MsgActionPositiveReviewsQuery)
	copy(message[2:], data)
	return message
}
