package top_positive_reviews

import (
	j "distribuidos-tp/internal/system_protocol/joiner"
	"sort"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type TopPositiveReviews struct {
	ReceiveMsg       func() (int, *j.JoinedPositiveGameReview, bool, error)
	SendQueryResults func(int, []*j.JoinedPositiveGameReview) error
	SendEof          func() error
}

func NewTopPositiveReviews(receiveMsg func() (int, *j.JoinedPositiveGameReview, bool, error), sendMetrics func(int, []*j.JoinedPositiveGameReview) error, sendEof func() error) *TopPositiveReviews {
	return &TopPositiveReviews{
		ReceiveMsg:       receiveMsg,
		SendQueryResults: sendMetrics,
		SendEof:          sendEof,
	}
}

func (t *TopPositiveReviews) Run(indieReviewJoinerAmount int) {
	remainingEOFsMap := make(map[int]int)
	topPositiveIndieGames := make(map[int][]*j.JoinedPositiveGameReview)

	for {
		clientID, msg, eof, err := t.ReceiveMsg()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		clientTopPositiveIndieGames, exists := topPositiveIndieGames[clientID]
		if !exists {
			clientTopPositiveIndieGames = []*j.JoinedPositiveGameReview{}
			topPositiveIndieGames[clientID] = clientTopPositiveIndieGames
		}

		if eof {
			log.Infof("Received EOF for client %d", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = indieReviewJoinerAmount
			}
			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			err = t.SendQueryResults(clientID, clientTopPositiveIndieGames)
			if err != nil {
				log.Errorf("Failed to send metrics: %v", err)
				return
			}
			log.Infof("Sent Top 5 positive reviews to writer")
			err = t.SendEof()
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}
			log.Info("Sent eof to writer")
			continue
		}

		log.Infof("Received indie game with ID: %v", msg.AppId)
		log.Infof("Evaluating number of positive reviews and saving game")
		clientTopPositiveIndieGames = append(clientTopPositiveIndieGames, msg)
		topPositiveIndieGames[clientID] = clientTopPositiveIndieGames
		if len(clientTopPositiveIndieGames) > 5 {
			// Sort the slice by positive reviews in descending order
			sort.Slice(clientTopPositiveIndieGames, func(i, j int) bool {
				return clientTopPositiveIndieGames[i].PositiveReviews > clientTopPositiveIndieGames[j].PositiveReviews
			})
			// Keep only the top 5
			clientTopPositiveIndieGames = clientTopPositiveIndieGames[:5]
			topPositiveIndieGames[clientID] = clientTopPositiveIndieGames
		}

	}

}
