package persistence

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	f "distribuidos-tp/internal/utils"
	"fmt"
	"strconv"
)

type Repository struct {
	PrefixFileName    string
	PrefixEofFileName string
}

func NewRepository(prefixFileName string) *Repository {
	return &Repository{
		PrefixFileName:    prefixFileName,
		PrefixEofFileName: "eof",
	}
}

func (r *Repository) Persist(clientID int, g oa.GameOSMetrics) {

	body := oa.SerializeGameOSMetrics(&g)
	f.TruncateAndWriteAllToFile(r.FileName(clientID), body)
}

func (r *Repository) Load(clientID int) (*oa.GameOSMetrics, error) {
	data, err := f.ReadAllFromFile(r.FileName(clientID))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return oa.NewGameOSMetrics(), nil
	}
	return oa.DeserializeGameOSMetrics(data)
}

func (r *Repository) FileName(clientID int) string {
	return fmt.Sprintf("%s_%d", r.PrefixFileName, clientID)
}

func (r *Repository) EofFileName(clientID int) string {
	return fmt.Sprintf("%s_%d", r.PrefixEofFileName, clientID)
}

func (r *Repository) PersistAndUpdateEof(clientID int, osAccumulatorAmount int) (int, error) {
	data, err := f.ReadAllFromFile(r.EofFileName(clientID))
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		f.TruncateAndWriteAllToFile(r.EofFileName(clientID), []byte(strconv.Itoa(osAccumulatorAmount-1)))
		return osAccumulatorAmount, nil
	}

	currentAmount, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, err
	}

	newAmount := currentAmount - 1
	f.TruncateAndWriteAllToFile(r.EofFileName(clientID), []byte(strconv.Itoa(newAmount)))
	return newAmount, nil
}
