package persistence

import (
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"errors"
	"github.com/op/go-logging"
	"os"
	"sync"
)

const (
	CopySuffix     = "_old"
	FileLengthSize = 8
)

type Persister[T any] struct {
	fileName    string
	serialize   func(T) []byte
	deserialize func([]byte) (T, error)
	wg          *sync.WaitGroup
	logger      *logging.Logger
}

func NewPersister[T any](fileName string, serialize func(T) []byte, deserialize func([]byte) (T, error), wg *sync.WaitGroup, logger *logging.Logger) *Persister[T] {
	return &Persister[T]{fileName, serialize, deserialize, wg, logger}
}

func (p *Persister[T]) Save(data T) error {
	serializedData := p.serialize(data)

	p.wg.Add(1)

	oldFileName := p.fileName + CopySuffix
	err := os.Rename(p.fileName, oldFileName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	file, err := os.OpenFile(p.fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			p.logger.Errorf("Error closing file: %v", err)
		}
		p.wg.Done()
	}()

	lengthBytes := make([]byte, FileLengthSize)
	binary.BigEndian.PutUint64(lengthBytes, uint64(len(serializedData)))

	err = u.WriteExact(file, lengthBytes)
	if err != nil {
		return err
	}

	err = u.WriteExact(file, serializedData)
	if err != nil {
		return err
	}

	return nil
}

func (p *Persister[T]) Load() (T, error) {
	p.wg.Add(1)

	file, err := os.OpenFile(p.fileName, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		var zero T
		return zero, err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			p.logger.Errorf("Error closing file: %v", err)
		}
		p.wg.Done()
	}()

	fileLengthBytes, err := u.ReadExact(file, FileLengthSize)
	if err != nil {
		var zero T
		return zero, err
	}

	fileLength := binary.BigEndian.Uint64(fileLengthBytes)
	data, err := u.ReadExact(file, int(fileLength))
	if err != nil {
		var zero T
		return zero, err
	}

	return p.deserialize(data)
}
