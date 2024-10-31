package system_protocol

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"

	j "distribuidos-tp/internal/system_protocol/joiner"

	df "distribuidos-tp/internal/system_protocol/decade_filter"
)

type Query byte

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

func SerializeMsgActionEnglishReviewsQuery(clientID int, joinedReviews []*j.JoinedNegativeGameReview) ([]byte, error) {
	data, err := j.SerializeJoinedActionNegativeGameReviewsBatch(joinedReviews)
	if err != nil {
		return nil, err
	}
	return SerializeQuery(MsgActionPositiveReviewsQuery, clientID, data), nil
}

func DeserializeMsgActionPositiveReviewsQuery(message []byte) ([]*j.JoinedNegativeGameReview, error) {
	return j.DeserializeJoinedActionNegativeGameReviewsBatch(message)
}

// Message ActionNegativeReviewsQuery

func SerializeMsgActionNegativeReviewsQuery(clientID int, joinedReviews []*j.JoinedNegativeGameReview) ([]byte, error) {
	data, err := j.SerializeJoinedActionNegativeGameReviewsBatch(joinedReviews)
	if err != nil {
		return nil, err
	}
	return SerializeQuery(MsgActionNegativeReviewsQuery, clientID, data), nil
}

func DeserializeMsgActionNegativeReviewsQuery(message []byte) ([]*j.JoinedNegativeGameReview, error) {
	return j.DeserializeJoinedActionNegativeGameReviewsBatch(message)
}

// Message TopTenResolvedQuery

func SerializeMsgTopTenResolvedQuery(clientID int, finalTopTenGames []*df.GameYearAndAvgPtf) []byte {
	data := df.SerializeTopTenAvgPlaytimeForever(finalTopTenGames)
	return SerializeQuery(MsgTopTenDecadeAvgPtfQuery, clientID, data)
}

func DeserializeMsgTopTenResolvedQuery(message []byte) ([]*df.GameYearAndAvgPtf, error) {
	return df.DeserializeTopTenAvgPlaytimeForever(message)

}
