package top_ten_accumulator

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type TopTenAccumulator struct {
	ReceiveMsg func() (int, []*df.GameYearAndAvgPtf, bool, error)
	SendMsg    func(int, []*df.GameYearAndAvgPtf) error
}

func NewTopTenAccumulator(receiveMsg func() (int, []*df.GameYearAndAvgPtf, bool, error), sendMsg func(int, []*df.GameYearAndAvgPtf) error) *TopTenAccumulator {
	return &TopTenAccumulator{
		ReceiveMsg: receiveMsg,
		SendMsg:    sendMsg,
	}
}

func (t *TopTenAccumulator) Run(decadeFilterAmount int, fileName string) {

	// remainingEOFs := decadeFilterAmount
	topTenGames := make(map[int][]*df.GameYearAndAvgPtf)
	remainingEOFs := make(map[int]int)

	for {

		clientID, decadeGames, eof, err := t.ReceiveMsg()
		if err != nil {
			log.Errorf("failed to receive message: %v", err)
			return
		}

		clientTopTenGames, exists := topTenGames[clientID]
		if !exists {
			clientTopTenGames = []*df.GameYearAndAvgPtf{}
			topTenGames[clientID] = clientTopTenGames
		}

		log.Infof("Received decade games")

		if eof {

			// Si no existe inicializo el contador de EOFs restantes
			if _, ok := remainingEOFs[clientID]; !ok {
				remainingEOFs[clientID] = decadeFilterAmount - 1
			} else {
				remainingEOFs[clientID]--

				if remainingEOFs[clientID] <= 0 {
					log.Infof("Received all EOFs of client %d, sending final top ten games", clientID)
					/*finalTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile("top_ten_games")
					if err != nil {
						log.Errorf("error uploading top ten games from file: %v", err)
						return
					}*/
					err = t.SendMsg(clientID, clientTopTenGames)
					if err != nil {
						log.Errorf("failed to send metrics: %v", err)
						return
					}
					delete(topTenGames, clientID)
					delete(remainingEOFs, clientID)
				}
			}
		}

		clientTopTenGames = df.TopTenAvgPlaytimeForever(append(clientTopTenGames, decadeGames...))
		topTenGames[clientID] = clientTopTenGames
		log.Infof("Updated top ten games for client %d", clientID)

		//topTenGames := df.TopTenAvgPlaytimeForever(decadeGames)

		// Upload the actual top ten games from the file and update the top ten games
		/*actualTopTenGames, err := df.UploadTopTenAvgPlaytimeForeverFromFile(fileName)
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
		}*/

	}
}
