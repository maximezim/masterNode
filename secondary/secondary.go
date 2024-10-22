package secondary

import (
	"encoding/json"
	"log"
	"sync"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type PacketRequest struct {
	VideoID      string `json:"video_id"`
	PacketNumber int    `json:"packet_number"`
	ChannelUUID  string `json:"channel_uuid"`
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
	log.Printf("Received PacketRequest: %+v", req)

	im.forwardPacketRequest(msg.Payload())
	im.subscribeToChannelUUID(req.ChannelUUID)
}

func (im *InterconnectManager) forwardPacketRequest(payload []byte) {
	for _, client := range im.clients {
		if client.client.IsConnected() {
			token := client.client.Publish("packet-request", 0, false, payload)
			token.Wait()
			if token.Error() != nil {
				log.Printf("Error publishing to broker %s: %v", client.brokerURI, token.Error())
			} else {
				log.Printf("Forwarded packet-request to broker %s", client.brokerURI)
			}
		} else {
			log.Printf("Cannot publish to broker %s: not connected", client.brokerURI)
		}
	}
}

func (im *InterconnectManager) subscribeToChannelUUID(channelUUID string) {
	for _, client := range im.clients {
		if client.client.IsConnected() {
			topic := channelUUID
			token := client.client.Subscribe(topic, 0, im.handleChannelUUIDMessage)
			token.Wait()
			if token.Error() != nil {
				log.Printf("Error subscribing to topic %s on broker %s: %v", topic, client.brokerURI, token.Error())
			} else {
				log.Printf("Subscribed to topic %s on broker %s", topic, client.brokerURI)
			}
		} else {
			log.Printf("Cannot subscribe to broker %s: not connected", client.brokerURI)
		}
	}
}

func (im *InterconnectManager) handleChannelUUIDMessage(client MQTT.Client, msg MQTT.Message) {
	log.Printf("Received message on topic %s from other broker, forwarding to main broker", msg.Topic())
	im.mainClient.Publish(msg.Topic(), msg.Qos(), msg.Retained(), msg.Payload())
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
