package node

import (
	u "distribuidos-tp/internal/utils"
	"fmt"
)

type EndOfFile struct {
	SenderID     int
	MessagesSent int
}

func NewEndOfFile(senderID int, messagesSent int) *EndOfFile {
	return &EndOfFile{
		SenderID:     senderID,
		MessagesSent: messagesSent,
	}
}

func (e *EndOfFile) Serialize() []byte {
	serialized := u.SerializeInt(e.SenderID)
	serialized = append(serialized, u.SerializeInt(e.MessagesSent)...)
	return serialized
}

func DeserializeEOF(data []byte) (*EndOfFile, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("data too short to deserialize into EOF")
	}

	senderID, err := u.DeserializeInt(data[:8])
	if err != nil {
		return nil, err
	}

	messagesSent, err := u.DeserializeInt(data[8:])
	if err != nil {
		return nil, err
	}

	return NewEndOfFile(senderID, messagesSent), nil
}
