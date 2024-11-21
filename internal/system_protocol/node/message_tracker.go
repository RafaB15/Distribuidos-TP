package node

import (
	u "distribuidos-tp/internal/utils"
	"fmt"
	"github.com/op/go-logging"
	"hash/fnv"
)

type MessageTracker struct {
	processedMessages *IntMap[*IntMap[bool]]
	remainingEOFsMap  *IntMap[int]
	expectedMessages  *IntMap[int]
	expectedEOFs      int
}

func NewMessageTracker(expectedEOFs int) *MessageTracker {
	boolMap := NewIntMap[bool](u.SerializeBool, u.DeserializeBool)
	return &MessageTracker{
		processedMessages: NewIntMap[*IntMap[bool]](boolMap.Serialize, boolMap.Deserialize),
		remainingEOFsMap:  NewIntMap[int](u.SerializeInt, u.DeserializeInt),
		expectedMessages:  NewIntMap[int](u.SerializeInt, u.DeserializeInt),
		expectedEOFs:      expectedEOFs,
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

func (m *MessageTracker) RegisterEOF(clientID int, endOfFile *EndOfFile, logger *logging.Logger) error {
	logger.Infof("Received EOF from client %d with %d messages", endOfFile.SenderID, endOfFile.MessagesSent)
	remainingEOFs, exists := m.remainingEOFsMap.Get(clientID)
	if !exists {
		remainingEOFs = m.expectedEOFs
	}

	remainingEOFs--
	if remainingEOFs < 0 {
		return fmt.Errorf("received more EOFs than expected")
	}

	m.remainingEOFsMap.Set(clientID, remainingEOFs)

	logger.Infof("Remaining EOFs for client %d: %d", clientID, remainingEOFs)

	clientExpectedMessages, _ := m.expectedMessages.Get(clientID)
	m.expectedMessages.Set(clientID, clientExpectedMessages+endOfFile.MessagesSent)

	logger.Infof("Client %d expected messages: %d", clientID, clientExpectedMessages+endOfFile.MessagesSent)

	return nil
}

func (m *MessageTracker) ClientFinished(clientID int, logger *logging.Logger) bool {
	remainingEOFs, exists := m.remainingEOFsMap.Get(clientID)

	if exists && remainingEOFs == 0 {
		clientExpectedMessages, _ := m.expectedMessages.Get(clientID)
		clientProcessedMessages, _ := m.processedMessages.Get(clientID)
		logger.Infof("Client %d processed messages: %d", clientID, clientProcessedMessages.Size())
		logger.Infof("Client %d expected messages: %d", clientID, clientExpectedMessages)
		logger.Infof("Client %d expected EOF: %d", clientID, m.expectedEOFs)
		return clientExpectedMessages == (clientProcessedMessages.Size() - m.expectedEOFs)
	}
	return false
}

func (m *MessageTracker) DeleteEOF(clientID int) {
	m.remainingEOFsMap.Delete(clientID)
}

func SerializeMessageTracker(m *MessageTracker) []byte {
	var serializedMessageTracker []byte

	serializedProcessedMessages := m.processedMessages.Serialize(m.processedMessages)
	length := len(serializedProcessedMessages)

	serializedMessageTracker = append(serializedMessageTracker, u.SerializeInt(length)...)
	serializedMessageTracker = append(serializedMessageTracker, serializedProcessedMessages...)

	serializedRemainingEOFsMap := m.remainingEOFsMap.Serialize(m.remainingEOFsMap)
	length = len(serializedRemainingEOFsMap)

	serializedMessageTracker = append(serializedMessageTracker, u.SerializeInt(length)...)
	serializedMessageTracker = append(serializedMessageTracker, serializedRemainingEOFsMap...)

	serializedExpectedMessages := m.expectedMessages.Serialize(m.expectedMessages)
	length = len(serializedExpectedMessages)

	serializedMessageTracker = append(serializedMessageTracker, u.SerializeInt(length)...)
	serializedMessageTracker = append(serializedMessageTracker, serializedExpectedMessages...)

	serializedExpectedEOFs := u.SerializeInt(m.expectedEOFs)

	serializedMessageTracker = append(serializedMessageTracker, serializedExpectedEOFs...)

	return serializedMessageTracker
}

func DeserializeMessageTracker(data []byte) (*MessageTracker, error) {
	boolMap := NewIntMap[bool](u.SerializeBool, u.DeserializeBool)
	processedMessages := NewIntMap[*IntMap[bool]](boolMap.Serialize, boolMap.Deserialize)

	remainingEOFsMap := NewIntMap[int](u.SerializeInt, u.DeserializeInt)
	expectedMessages := NewIntMap[int](u.SerializeInt, u.DeserializeInt)

	offset := 0

	length, err := u.DeserializeInt(data[offset:])
	if err != nil {
		return nil, err
	}

	offset += 4

	processedMessagesData := data[offset : offset+length]
	deserializedProcessedMessages, err := processedMessages.Deserialize(processedMessagesData)
	if err != nil {
		return nil, err
	}

	offset += length

	length, err = u.DeserializeInt(data[offset:])
	if err != nil {
		return nil, err
	}

	offset += 8

	remainingEOFsMapData := data[offset : offset+length]
	deserializedRemainingEOFsMap, err := remainingEOFsMap.Deserialize(remainingEOFsMapData)
	if err != nil {
		return nil, err
	}

	offset += length

	length, err = u.DeserializeInt(data[offset:])
	if err != nil {
		return nil, err
	}

	offset += 8

	expectedMessagesData := data[offset : offset+length]
	deserializedExpectedMessages, err := expectedMessages.Deserialize(expectedMessagesData)
	if err != nil {
		return nil, err
	}

	offset += length

	expectedEOFs, err := u.DeserializeInt(data[offset:])
	if err != nil {
		return nil, err
	}

	return &MessageTracker{
		processedMessages: deserializedProcessedMessages,
		remainingEOFsMap:  deserializedRemainingEOFsMap,
		expectedMessages:  deserializedExpectedMessages,
		expectedEOFs:      expectedEOFs,
	}, nil
}
