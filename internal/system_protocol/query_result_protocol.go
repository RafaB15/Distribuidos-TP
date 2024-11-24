package system_protocol

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"

	j "distribuidos-tp/internal/system_protocol/joiner"
)

type Query byte

const (
	MsgOsResolvedQuery Query = iota
	MsgTopTenDecadeAvgPtfQuery
	MsgIndiePositiveJoinedReviewsQuery
	MsgActionNegativeEnglishReviewsQuery
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

// Message TopTenResolvedQuery

func SerializeMsgTopTenResolvedQuery(clientID int, finalTopTenGames []*df.GameYearAndAvgPtf) []byte {
	data := df.SerializeTopTenAvgPlaytimeForever(finalTopTenGames)
	return SerializeQuery(MsgTopTenDecadeAvgPtfQuery, clientID, data)
}

func DeserializeMsgTopTenResolvedQuery(message []byte) ([]*df.GameYearAndAvgPtf, error) {
	return df.DeserializeTopTenAvgPlaytimeForever(message)

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

// Message MsgActionNegativeEnglishReviewsQuery

func SerializeMsgActionNegativeEnglishReviewsQuery(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) []byte {
	data := ra.SerializeNamedGameReviewsMetricsBatch(namedGameReviewsMetricsBatch)
	return SerializeQuery(MsgActionNegativeEnglishReviewsQuery, clientID, data)
}

func DeserializeMsgActionNegativeEnglishReviewsQuery(message []byte) ([]*ra.NamedGameReviewsMetrics, error) {
	return ra.DeserializeNamedGameReviewsMetricsBatch(message)
}

// Message ActionNegativeReviewsQuery

func SerializeMsgActionNegativeReviewsQuery(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) []byte {
	data := ra.SerializeNamedGameReviewsMetricsBatch(namedGameReviewsMetricsBatch)
	return SerializeQuery(MsgActionNegativeReviewsQuery, clientID, data)
}

func DeserializeMsgActionNegativeReviewsQuery(message []byte) ([]*ra.NamedGameReviewsMetrics, error) {
	return ra.DeserializeNamedGameReviewsMetricsBatch(message)
}
