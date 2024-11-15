package persistence

import (
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"errors"
	"os"
)

const (
	CopySuffix     = "_old"
	FileLengthSize = 8
)

type Persister[T any] struct {
	fileName    string
	serialize   func(*T) ([]byte, error)
	deserialize func([]byte) (*T, error)
}

func NewPersister[T any](fileName string, serialize func(*T) ([]byte, error), deserialize func([]byte) (*T, error)) *Persister[T] {
	return &Persister[T]{fileName, serialize, deserialize}
}

func (p *Persister[T]) Save(data *T) error {
	serializedData, err := p.serialize(data)
	if err != nil {
		return err
	}

	oldFileName := p.fileName + CopySuffix
	err = os.Rename(p.fileName, oldFileName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	file, err := os.OpenFile(p.fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

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

func (p *Persister[T]) Load() (*T, error) {
	file, err := os.OpenFile(p.fileName, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileLengthBytes, err := u.ReadExact(file, FileLengthSize)
	if err != nil {
		return nil, err
	}

	fileLength := binary.BigEndian.Uint64(fileLengthBytes)
	data, err := u.ReadExact(file, int(fileLength))
	if err != nil {
		return nil, err
	}

	return p.deserialize(data)
}
