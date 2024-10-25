package final_positive_joiner

import (
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type FinalPositiveJoiner struct {
	ReceiveJoinedGameReviews func() (int, *j.JoinedPositiveGameReview, bool, error)
	SendMetrics              func(int, []*j.JoinedPositiveGameReview) error
}

func NewFinalPositiveJoiner(receiveJoinedGameReviews func() (int, *j.JoinedPositiveGameReview, bool, error), sendMetrics func(int, []*j.JoinedPositiveGameReview) error) *FinalPositiveJoiner {
	return &FinalPositiveJoiner{
		ReceiveJoinedGameReviews: receiveJoinedGameReviews,
		SendMetrics:              sendMetrics,
	}
}

func (f *FinalPositiveJoiner) Run(actionPositiveJoinersAmount int) {
	remainingEOFsMap := make(map[int]int)
	accumulatedGameReviews := make(map[int][]*j.JoinedPositiveGameReview)

	for {
		clientID, joinedGamesReviews, eof, err := f.ReceiveJoinedGameReviews()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		clientAccumulatedGameReviews, exists := accumulatedGameReviews[clientID]
		if !exists {
			clientAccumulatedGameReviews = []*j.JoinedPositiveGameReview{}
			accumulatedGameReviews[clientID] = clientAccumulatedGameReviews
		}

		if eof {
			log.Infof("Received EOF for client %d", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = actionPositiveJoinersAmount
			}

			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}

			log.Infof("Received all EOFs of client: %d, sending EOF to entrypoint", clientID)
			err = f.SendMetrics(clientID, clientAccumulatedGameReviews)
			if err != nil {
				log.Errorf("Failed to send metrics: %v", err)
				return
			}

			delete(accumulatedGameReviews, clientID)
			delete(remainingEOFsMap, clientID)
			continue
		}

		log.Infof("Received joined negative game reviews from client %d", clientID)
		clientAccumulatedGameReviews = append(clientAccumulatedGameReviews, joinedGamesReviews)
		accumulatedGameReviews[clientID] = clientAccumulatedGameReviews
		log.Infof("Saved joined negative game reviews from client %d", clientID)
	}
}
