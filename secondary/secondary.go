// secondary.go
package secondary

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type PacketRequest struct {
	VideoID      string `json:"video_id"`
	PacketNumber int    `json:"packet_number"`
	ChannelUUID  string `json:"channel_uuid"`
	IsForwarded  bool   `json:"is_forwarded"` // Add this field
}

type InterconnectManager struct {
	otherBrokers []string
	mainClient   MQTT.Client
	clients      []*OtherMQTTClient
	wg           sync.WaitGroup
	stopChan     chan struct{}
}

type OtherMQTTClient struct {
	brokerURI string
	client    MQTT.Client
	manager   *InterconnectManager
}

func NewInterconnectManager(brokers []string, mainClient MQTT.Client) *InterconnectManager {
	return &InterconnectManager{
		otherBrokers: brokers,
		mainClient:   mainClient,
		clients:      make([]*OtherMQTTClient, 0, len(brokers)),
		stopChan:     make(chan struct{}),
	}
}

func (im *InterconnectManager) Start() {
	for _, broker := range im.otherBrokers {
		client := &OtherMQTTClient{
			brokerURI: broker,
			manager:   im,
		}
		client.init()
		im.clients = append(im.clients, client)
	}
}

func (im *InterconnectManager) Stop() {
	close(im.stopChan)
	for _, client := range im.clients {
		if client.client.IsConnected() {
			client.client.Disconnect(250)
			log.Printf("Disconnected from broker %s", client.brokerURI)
		}
	}
	im.wg.Wait()
}

func (im *InterconnectManager) HandlePacketRequest(client MQTT.Client, msg MQTT.Message) {
	var req PacketRequest
	err := json.Unmarshal(msg.Payload(), &req)
	if err != nil {
		log.Printf("Error unmarshalling PacketRequest: %v", err)
		return
	}

	// Skip if this is a forwarded request to prevent loops
	if req.IsForwarded {
		log.Printf("Received forwarded request, skipping to prevent loops: %+v", req)
		return
	}

	log.Printf("Received PacketRequest: %+v", req)
	
	// Subscribe to both the original channel UUID and server-prefixed version
	im.subscribeToChannelUUID(req.ChannelUUID)
	im.subscribeToChannelUUID("server" + req.ChannelUUID)

	// Mark the request as forwarded before sending to other nodes
	req.IsForwarded = true
	forwardPayload, err := json.Marshal(req)
	if err != nil {
		log.Printf("Error marshalling forwarded request: %v", err)
		return
	}
	im.forwardPacketRequest(forwardPayload)
}

// forwardPacketRequest publishes the packet-request to all other brokers
func (im *InterconnectManager) forwardPacketRequest(payload []byte) {
	for _, client := range im.clients {
		if client.client.IsConnected() {
			token := client.client.Publish("packet-request", 0, false, payload)
			if token.Error() != nil {
				log.Printf("Error publishing to broker %s", client.brokerURI)
			} else {
				log.Printf("Forwarded packet-request to broker %s", client.brokerURI)
			}
		} else {
			log.Printf("Cannot publish to broker %s: not connected", client.brokerURI)
		}
	}
}

func (im *InterconnectManager) subscribeToChannelUUID(channelUUID string) {
	// Subscribe on the main client to handle local messages
	token := im.mainClient.Subscribe(channelUUID, 0, im.handleChannelUUIDMessage)
	token.Wait()
	if token.Error() != nil {
		log.Printf("Error subscribing to topic %s on main broker: %v", channelUUID, token.Error())
	} else {
		log.Printf("Subscribed to topic %s on main broker", channelUUID)
	}

	// Subscribe on all other clients
	for _, client := range im.clients {
		if client.client.IsConnected() {
			token := client.client.Subscribe(channelUUID, 0, im.handleChannelUUIDMessage)
			token.Wait()
			if token.Error() != nil {
				log.Printf("Error subscribing to topic %s on broker %s: %v", channelUUID, client.brokerURI, token.Error())
			} else {
				log.Printf("Subscribed to topic %s on broker %s", channelUUID, client.brokerURI)
			}
		}
	}
}

func (im *InterconnectManager) handleChannelUUIDMessage(client MQTT.Client, msg MQTT.Message) {
	topic := msg.Topic()
	payload := msg.Payload()
	
	if strings.HasPrefix(topic, "server") {
		// Message from another broker - forward to local broker without "server" prefix
		originalTopic := strings.TrimPrefix(topic, "server")
		log.Printf("Received message on topic %s from other broker, forwarding to main broker as %s", topic, originalTopic)
		token := im.mainClient.Publish(originalTopic, msg.Qos(), msg.Retained(), payload)
		if token.Wait() && token.Error() != nil {
			log.Printf("Error forwarding message to main broker: %v", token.Error())
		}
	} else {
		// Message from local broker - forward to other brokers with "server" prefix
		serverTopic := "server" + topic
		log.Printf("Received message on topic %s from main broker, forwarding to other brokers as %s", topic, serverTopic)
		for _, client := range im.clients {
			if client.client.IsConnected() {
				token := client.client.Publish(serverTopic, msg.Qos(), msg.Retained(), payload)
				if token.Wait() && token.Error() != nil {
					log.Printf("Error forwarding message to broker %s: %v", client.brokerURI, token.Error())
				}
			}
		}
	}
}

func (oc *OtherMQTTClient) init() {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(oc.brokerURI)
	opts.SetClientID("")
	opts.SetUsername("")
	opts.SetPassword("")
	opts.SetCleanSession(true)
	opts.OnConnectionLost = func(client MQTT.Client, err error) {
		log.Printf("Connection lost to broker %s: %v", oc.brokerURI, err)
	}
	opts.OnReconnecting = func(client MQTT.Client, opts *MQTT.ClientOptions) {
		log.Printf("Reconnecting to broker %s...", oc.brokerURI)
	}

	oc.client = MQTT.NewClient(opts)
	if token := oc.client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("Error connecting to broker %s: %v", oc.brokerURI, token.Error())
	} else {
		log.Printf("Connected to broker %s", oc.brokerURI)
	}
}
