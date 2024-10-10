package message

// Message represents an MQTT message
type Message struct {
	Topic   string
	Payload []byte
}
