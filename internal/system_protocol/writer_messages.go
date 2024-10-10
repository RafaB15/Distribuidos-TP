package system_protocol

import "fmt"

type Query byte

const (
	MsgOsResolvedQuery Query = iota
	MsgTopTenDecadeAvgPtfQuery
	MsgIndiePositiveJoinedReviewsQuery
	MsgActionPositiveReviewsQuery
	MsgActionNegativeReviewsQuery
)

func DeserializeQueryResolvedType(message []byte) (Query, error) {
	if len(message) == 0 {
		return 0, fmt.Errorf("empty message")
	}

	query := Query(message[0])
	switch query {

	case MsgOsResolvedQuery, MsgActionPositiveReviewsQuery, MsgTopTenDecadeAvgPtfQuery, MsgIndiePositiveJoinedReviewsQuery:
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

func SerializeTopTenDecadeAvgPtfQueryMsg(data []byte) []byte {
	message := make([]byte, 2+len(data))
	message[0] = byte(MsgQueryResolved)
	message[1] = byte(MsgTopTenDecadeAvgPtfQuery)
	copy(message[2:], data)
	return message
}
