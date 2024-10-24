package system_protocol

import oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"

const (
	MsgOsResolvedQuery Query = iota
	MsgTopTenDecadeAvgPtfQuery
	MsgIndiePositiveJoinedReviewsQuery
	MsgActionPositiveReviewsQuery
	MsgActionNegativeReviewsQuery
)

// Message OsResolvedQuery

func SerializeMsgOsResolvedQuery(clientID int, gameMetrics *oa.GameOSMetrics) []byte {
	data := oa.SerializeGameOSMetrics(gameMetrics)
	return SerializeQuery(MsgOsResolvedQuery, clientID, data)
}

func DeserializeMsgOsResolvedQuery(message []byte) (*oa.GameOSMetrics, error) {
	return oa.DeserializeGameOSMetrics(message)
}
