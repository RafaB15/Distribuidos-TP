package node

import (
	u "distribuidos-tp/internal/utils"
	"hash/fnv"
)

type MessageTracker struct {
	processedMessages *IntMap[*IntMap[bool]]
}

func NewMessageTracker() *MessageTracker {
	boolMap := NewIntMap[bool](u.SerializeBool, u.DeserializeBool)
	return &MessageTracker{
		processedMessages: NewIntMap[*IntMap[bool]](boolMap.Serialize, boolMap.Deserialize),
	}
}

func (m *MessageTracker) ProcessMessage(clientID int, messageBody []byte) (newMessage bool, errResponse error) {
	clientMessages, exists := m.processedMessages.Get(clientID)
	if !exists {
		clientMessages = NewIntMap[bool](u.SerializeBool, u.DeserializeBool)
		m.processedMessages.Set(clientID, clientMessages)
	}

	messageHash := fnv.New64a()
	_, err := messageHash.Write(messageBody)
	if err != nil {
		return false, err
	}

	messageHashValue := int(messageHash.Sum64())
	_, exists = clientMessages.Get(messageHashValue)

	if exists {
		return false, nil
	}

	clientMessages.Set(messageHashValue, true)
	return true, nil
}

func Serialize(m *MessageTracker) []byte {
	return m.processedMessages.Serialize(m.processedMessages)
}

func Deserialize(data []byte) (*MessageTracker, error) {
	boolMap := NewIntMap[bool](u.SerializeBool, u.DeserializeBool)
	processedMessages := NewIntMap[*IntMap[bool]](boolMap.Serialize, boolMap.Deserialize)
	deserializedMap, err := processedMessages.Deserialize(data)
	if err != nil {
		return nil, err
	}
	return &MessageTracker{
		processedMessages: deserializedMap,
	}, nil
}
