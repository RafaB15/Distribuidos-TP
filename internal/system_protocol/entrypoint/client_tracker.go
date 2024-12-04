package entrypoint

type ClientTracker struct {
	NextClientID       int
	SendingInformation map[int]bool
	AwaitingResponse   map[int][]int
}

func NewClientTracker() *ClientTracker {
	return &ClientTracker{
		NextClientID:       1,
		SendingInformation: make(map[int]bool),
		AwaitingResponse:   make(map[int][]int),
	}
}

func (c *ClientTracker) AddClient() int {
	clientID := c.NextClientID
	c.NextClientID++
	c.SendingInformation[clientID] = true
	return clientID
}

func (c *ClientTracker) StartAwaiting(clientID int) {
	delete(c.SendingInformation, clientID)
	c.AwaitingResponse[clientID] = make([]int, 0)
}

func (c *ClientTracker) AddQuery(clientID, queryID int) {
	c.AwaitingResponse[clientID] = append(c.AwaitingResponse[clientID], queryID)
}

func (c *ClientTracker) IsFinished(clientID int, queriesAmount int) bool {
	return len(c.AwaitingResponse[clientID]) == queriesAmount
}

func (c *ClientTracker) IsQueryReceived(clientID, queryID int) bool {
	for _, query := range c.AwaitingResponse[clientID] {
		if query == queryID {
			return true
		}
	}
	return false
}

func (c *ClientTracker) Finish(clientID int) {
	delete(c.AwaitingResponse, clientID)
}

func (c *ClientTracker) Serialize() []byte {
	buffer := make([]byte, 1)

	buffer[0] = byte(c.NextClientID)

	sendingInformationLength := len(c.SendingInformation)
	buffer = append(buffer, byte(sendingInformationLength))

	for clientID := range c.SendingInformation {
		buffer = append(buffer, byte(clientID))
	}

	awaitingResponseLength := len(c.AwaitingResponse)
	buffer = append(buffer, byte(awaitingResponseLength))

	for clientID, queriesReceived := range c.AwaitingResponse {
		buffer = append(buffer, byte(clientID))

		queriesReceivedLength := len(queriesReceived)
		buffer = append(buffer, byte(queriesReceivedLength))

		for _, queryID := range queriesReceived {
			buffer = append(buffer, byte(queryID))
		}
	}

	return buffer
}

func DeserializeClientTracker(data []byte) (*ClientTracker, error) {
	offset := 0

	nextClientID := int(data[offset])
	offset++

	sendingInformationLength := int(data[offset])
	offset++

	sendingInformation := make(map[int]bool, sendingInformationLength)
	for i := 0; i < sendingInformationLength; i++ {
		clientID := int(data[offset])
		offset++
		sendingInformation[clientID] = true
	}

	awaitingResponseLength := int(data[offset])
	offset++

	awaitingResponse := make(map[int][]int, awaitingResponseLength)
	for i := 0; i < awaitingResponseLength; i++ {
		clientID := int(data[offset])
		offset++

		queriesReceivedLength := int(data[offset])
		offset++
		queriesReceived := make([]int, queriesReceivedLength)
		for j := 0; j < queriesReceivedLength; j++ {
			queryID := int(data[offset])
			offset++
			queriesReceived[j] = queryID
		}

		awaitingResponse[clientID] = queriesReceived
	}

	return &ClientTracker{
		NextClientID:       nextClientID,
		SendingInformation: sendingInformation,
		AwaitingResponse:   awaitingResponse,
	}, nil
}
