package persistence

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	f "distribuidos-tp/internal/utils"
	"fmt"
)

type Repository struct {
	PrefixFileName string
}

func NewRepository(prefixFileName string) *Repository {
	return &Repository{
		PrefixFileName: prefixFileName,
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
