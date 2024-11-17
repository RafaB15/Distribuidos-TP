package persistence

import (
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"errors"
	"fmt"
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
	p.logger.Infof("Renaming %s to %s", p.fileName, oldFileName)
	err := os.Rename(p.fileName, oldFileName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	p.logger.Infof("Saving data to %s", p.fileName)
	p.logger.Infof("Saving data to %s", p.fileName)
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
	var zero T
	p.wg.Add(1)
	defer p.wg.Done()

	// We try to load the primary file
	data, err := p.loadFile(p.fileName)
	if err == nil {
		p.logger.Infof("Loaded data from primary file: %s", p.fileName)
		return p.deserialize(data)
	} else {
		p.logger.Errorf("Failed to load data from primary file: %v", err)
	}

	// If there were problems with the primary, we try to load the backup file
	data, err = p.loadFile(p.fileName + CopySuffix)
	if err == nil {
		p.logger.Infof("Loaded data from backup file: %s", p.fileName+CopySuffix)
		return p.deserialize(data)
	} else {
		p.logger.Errorf("Failed to load data from backup file: %v", err)
	}

	return zero, fmt.Errorf("failed to load data from both primary and backup files: %v", err)
}

func (p *Persister[T]) loadFile(fileName string) ([]byte, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			p.logger.Errorf("Error closing file: %v", err)
		}
	}()

	fileLengthBytes, err := u.ReadExact(file, FileLengthSize)
	if err != nil {
		return nil, err
	}

	fileLength := binary.BigEndian.Uint64(fileLengthBytes)
	data, err := u.ReadExact(file, int(fileLength))
	if err != nil {
		return nil, err
	}

	return data, nil
}
