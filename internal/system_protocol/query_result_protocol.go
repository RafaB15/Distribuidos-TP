package system_protocol

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	j "distribuidos-tp/internal/system_protocol/joiner"
)

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

// Message IndiePositiveJoinedReviewsQuery

func SerializeMsgIndiePositiveJoinedReviewsQuery(clientID int, joinedReviews []*j.JoinedPositiveGameReview) ([]byte, error) {
	data, err := j.SerializeJoinedPositiveGameReviewsBatch(joinedReviews)
	if err != nil {
		return nil, err
	}
	return SerializeQuery(MsgIndiePositiveJoinedReviewsQuery, clientID, data), nil
}

func DeserializeMsgIndiePositiveJoinedReviewsQuery(message []byte) ([]*j.JoinedPositiveGameReview, error) {
	return j.DeserializeJoinedPositiveGameReviewsBatch(message)
}

// Message ActionPositiveReviewsQuery

func SerializeMsgActionPositiveReviewsQuery(clientID int, joinedReviews []*j.JoinedPositiveGameReview) ([]byte, error) {
	data, err := j.SerializeJoinedPositiveGameReviewsBatch(joinedReviews)
	if err != nil {
		return nil, err
	}
	return SerializeQuery(MsgActionPositiveReviewsQuery, clientID, data), nil
}

func DeserializeMsgActionPositiveReviewsQuery(message []byte) ([]*j.JoinedPositiveGameReview, error) {
	return j.DeserializeJoinedPositiveGameReviewsBatch(message)
}
