package main

import (
	"encoding/json"
	"log"
	"masterNode/loadbalancer"
	"masterNode/message"
	"masterNode/secondary"
	"masterNode/waitinglist"
	"masterNode/worker"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var (
	brokerURI        = os.Getenv("MQTT_BROKER_URI")
	clientID         = os.Getenv("MQTT_CLIENT_ID")
	username         = os.Getenv("MQTT_USERNAME")
	password         = os.Getenv("MQTT_PASSWORD")
	otherEdgesEnv    = os.Getenv("OTHER_EDGES")
	loadBalancerIP   = ":8081"
	maxWorkers       = 10  // Maximum number of concurrent workers
	messageQueueSize = 256 // Size of the message queue
)

func main() {
	var other_edges []string
	err := json.Unmarshal([]byte(otherEdgesEnv), &other_edges)
	if err != nil {
		log.Printf("Error unmarshalling OTHER_EDGES: %v", err)
		other_edges = []string{}
	}
	log.Printf("Other edges: %v", other_edges)

	// Create a channel to capture interrupt signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Buffered channel to act as a message queue
	messageChan := waitinglist.NewWaitingList(messageQueueSize)

	wm := worker.NewWorkerManager()

	// Start the WebSocket server in a goroutine
	go func() {
		http.HandleFunc("/ws", worker.WorkerWebSocketHandler(wm))
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	policyHandler := loadbalancer.NewPolicyHandler(loadBalancerIP)

	// Start accepting load balancer connections
	go func() {
		err := policyHandler.AcceptLoadBalancer()
		if err != nil {
			log.Fatalf("Failed to accept load balancer connection: %v", err)
		}
		log.Println("Load balancer connected")
		err = policyHandler.SyncPolicy()
		if err != nil {
			log.Fatalf("Failed to sync policy: %v", err)
		}
	}()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Start worker pool
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ProcessMessageWorker(&messageChan, wm, &policyHandler)
		}()
	}

	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerURI)
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetCleanSession(false)
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		if shouldExclude(msg.Topic()) || msg.Topic() == "packet-request" {
			return
		}
		// Send the message to the message channel for processing
		messageChan.AddContent(message.Message{Topic: msg.Topic(), Payload: msg.Payload()})
	})
	opts.OnConnectionLost = func(client MQTT.Client, err error) {
		log.Printf("Connection lost: %v", err)
	}
	opts.OnReconnecting = func(client MQTT.Client, opts *MQTT.ClientOptions) {
		log.Printf("Attempting to reconnect...")
	}

	mainClient := MQTT.NewClient(opts)
	if token := mainClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to broker: %v", token.Error())
	}
	defer mainClient.Disconnect(250)
	log.Println("Connected to MQTT broker")

	// Subscribe to all topics
	if token := mainClient.Subscribe("#", 0, nil); token.Wait() && token.Error() != nil {
		log.Fatalf("Error subscribing to topics: %v", token.Error())
	}
	log.Println("Subscribed to all topics")

	// Initialize InterconnectManager for secondary master nodes
	interconnectManager := secondary.NewInterconnectManager(other_edges, mainClient)
	interconnectManager.Start()

	// Subscribe to "packet-request" with a specific handler
	if token := mainClient.Subscribe("packet-request", 0, interconnectManager.HandlePacketRequest); token.Wait() && token.Error() != nil {
		log.Fatalf("Error subscribing to 'packet-request' topic: %v", token.Error())
	}
	log.Println("Subscribed to 'packet-request' topic")

	// Wait for interrupt signal
	<-sigChan
	log.Println("Interrupt signal received, shutting down...")

	// Clean up
	messageChan.CleanUp() // Close the message channel to stop workers
	wg.Wait()             // Wait for all workers to finish

	interconnectManager.Stop()

	log.Println("Shutdown complete")
}

type VideoPacketSIS struct {
	MsgPackPacket []byte `msgpack:"packet"`
	A             []byte `msgpack:"a"`
	V             []byte `msgpack:"v"`
}

type VideoPacket struct {
	VideoID      string `json:"video_id"`
	PacketNumber int    `json:"packet_number"`
	TotalPackets int    `json:"total_packets"` // Use 0 if unknown
	Data         []byte `json:"data"`
}
