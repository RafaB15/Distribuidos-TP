package entrypoint

type ClientTracker struct {
	NextClientID   int
	CurrentClients map[int]bool
}

func NewClientTracker() *ClientTracker {
	return &ClientTracker{
		NextClientID:   1,
		CurrentClients: make(map[int]bool),
	}
}

func (c *ClientTracker) AddClient() int {
	clientID := c.NextClientID
	c.NextClientID++
	c.CurrentClients[clientID] = true
	return clientID
}

func (c *ClientTracker) FinishClient(clientID int) {
	delete(c.CurrentClients, clientID)
}

func SerializeClientTracker(c *ClientTracker) []byte {
	buffer := make([]byte, 1)

	buffer[0] = byte(c.NextClientID)

	sendingInformationLength := len(c.CurrentClients)
	buffer = append(buffer, byte(sendingInformationLength))

	for clientID := range c.CurrentClients {
		buffer = append(buffer, byte(clientID))
	}

	return buffer
}

func DeserializeClientTracker(data []byte) (*ClientTracker, error) {
	offset := 0

	nextClientID := int(data[offset])
	offset++

	currentClientsLength := int(data[offset])
	offset++

	currentClients := make(map[int]bool, currentClientsLength)
	for i := 0; i < currentClientsLength; i++ {
		clientID := int(data[offset])
		offset++
		currentClients[clientID] = true
	}

	return &ClientTracker{
		NextClientID:   nextClientID,
		CurrentClients: currentClients,
	}, nil
}
