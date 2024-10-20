package system_protocol

type Message struct {
	MessageType int
	ClientID    int
	Body        []byte
}
