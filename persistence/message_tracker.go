package persistence

type MessageTracker struct {
	processedMessages map[int]bool
}

func NewMessageTracker() *MessageTracker {
	return &MessageTracker{
		processedMessages: make(map[int]bool),
	}
}

func (m *MessageTracker) ProcessMessage(messageID int) {
	m.processedMessages[messageID] = true
}

func (m *MessageTracker) IsMessageProcessed(messageID int) bool {
	return m.processedMessages[messageID]
}
