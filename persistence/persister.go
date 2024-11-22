package persistence

import (
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"os"
	"sync"
)

const (
	CopySuffix     = "_old"
	FileLengthSize = 8
	SyncNumberSize = 8
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

func (p *Persister[T]) Save(data T, syncNumber uint64) error {
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

	syncNumberBytes := u.SerializeInt(int(syncNumber))

	err = u.WriteExact(file, syncNumberBytes)
	if err != nil {
		return err
	}

	lengthBytes := u.SerializeInt(len(serializedData))

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

func (p *Persister[T]) LoadPrimaryFile() (T, uint64, error) {
	return p.loadFile(p.fileName)
}

func (p *Persister[T]) LoadBackupFile() (T, uint64, error) {
	return p.loadFile(p.fileName + CopySuffix)
}

func (p *Persister[T]) Load() (T, uint64, error) {
	var zero T
	p.wg.Add(1)
	defer p.wg.Done()

	// We try to load the primary file
	persistedStructure, syncNumber, err := p.loadFile(p.fileName)
	if err == nil {
		p.logger.Infof("Loaded data from primary file: %s", p.fileName)
		return persistedStructure, syncNumber, nil
	} else {
		p.logger.Errorf("Failed to load data from primary file: %v", err)
	}

	// If there were problems with the primary, we try to load the backup file
	persistedStructure, syncNumber, err = p.loadFile(p.fileName)
	if err == nil {
		p.logger.Infof("Loaded data from backup file: %s", p.fileName+CopySuffix)
		return persistedStructure, syncNumber, nil
	} else {
		p.logger.Errorf("Failed to load data from backupfile file: %v", err)
	}

	return zero, 0, fmt.Errorf("failed to load data from both primary and backup files: %v", err)
}

func (p *Persister[T]) loadFile(fileName string) (T, uint64, error) {
	var zero T

	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return zero, 0, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			p.logger.Errorf("Error closing file: %v", err)
		}
	}()

	syncNumberBytes, err := u.ReadExact(file, SyncNumberSize)
	if err != nil {
		return zero, 0, err
	}
	syncNumber := binary.BigEndian.Uint64(syncNumberBytes)

	fileLengthBytes, err := u.ReadExact(file, FileLengthSize)
	if err != nil {
		return zero, 0, err
	}
	fileLength := binary.BigEndian.Uint64(fileLengthBytes)

	data, err := u.ReadExact(file, int(fileLength))
	if err != nil {
		return zero, 0, err
	}

	deserializedStruct, err := p.deserialize(data)
	if err != nil {
		return zero, 0, err
	}

	return deserializedStruct, syncNumber, nil
}

func (p *Persister[T]) Rollback() error {
	p.logger.Infof("Rolling back %s", p.fileName)

	srcFile, err := os.Open(p.fileName + CopySuffix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			p.logger.Warningf("Backup file %s does not exist", p.fileName+CopySuffix)
			return nil
		}
		return err
	}
	defer func() {
		err := srcFile.Close()
		if err != nil {
			p.logger.Errorf("Error closing src file: %v", err)
		}
	}()

	destFile, err := os.Create(p.fileName)
	if err != nil {
		return err
	}
	defer func() {
		err := destFile.Close()
		if err != nil {
			p.logger.Errorf("Error closing dest file: %v", err)
		}
	}()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}

	return nil
}
