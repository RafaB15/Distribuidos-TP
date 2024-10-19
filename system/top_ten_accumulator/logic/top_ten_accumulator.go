package top_ten_accumulator

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type TopTenAccumulator struct {
	ReceiveMsg func() ([]*df.GameYearAndAvgPtf, bool, error)
	SendMsg    func([]*df.GameYearAndAvgPtf) error
}

func NewTopTenAccumulator(receiveMsg func() ([]*df.GameYearAndAvgPtf, bool, error), sendMsg func([]*df.GameYearAndAvgPtf) error) *TopTenAccumulator {
	return &TopTenAccumulator{
		ReceiveMsg: receiveMsg,
		SendMsg:    sendMsg,
	}
}

func (t *TopTenAccumulator) Run(decadeFilterAmount int, fileName string) {

	remainingEOFs := decadeFilterAmount
	for {

		decadeGames, eof, err := t.ReceiveMsg()
		if err != nil {
			log.Errorf("failed to receive message: %v", err)
			return
		}

		if eof {
			remainingEOFs--
		}

		topTenGames := df.TopTenAvgPlaytimeForever(decadeGames)

		// Upload the actual top ten games from the file and update the top ten games
		actualTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile(fileName)
		if err != nil {
			log.Errorf("Error uploading top ten games from file: %v", err)
			return
		}
		updatedTopTenGames := df.TopTenAvgPlaytimeForever(append(topTenGames, actualTopTenGames...))

		// Save the updated top ten games to the file
		err = df.SaveTopTenAvgPlaytimeForeverToFile(updatedTopTenGames, "top_ten_games")
		if err != nil {
			log.Errorf("error saving top ten games to file: %v", err)
			return
		}

		// If there are no more EOFs, send the final top ten games
		if remainingEOFs <= 0 {
			finalTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile("top_ten_games")
			if err != nil {
				log.Errorf("error uploading top ten games from file: %v", err)
				return
			}
			err = t.SendMsg(finalTopTenGames)
			if err != nil {
				log.Errorf("failed to send metrics: %v", err)
				return
			}
		}
	}
}
