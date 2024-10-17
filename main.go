package main

import (
	"fmt"
	"log"
	"masterNode/loadbalancer"
	"masterNode/message"
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
	loadBalancerIP   = ":8081"
	excludedTopics   = []string{"worker/node/", "-stream", "-ping", "-stats"} // Topics to exclude
	maxWorkers       = 10                                                     // Maximum number of concurrent workers
	messageQueueSize = 256                                                    // Size of the message queue
)

func main() {
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
		fmt.Println("Load balancer connected")
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
	opts.SetKeepAlive(60 * time.Second) // Ensure keep-alive is set appropriately
	opts.SetPingTimeout(10 * time.Second)
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		// Filter out messages from excluded topics
		if shouldExclude(msg.Topic()) {
			fmt.Printf("Excluded message: %s\n", msg.Topic())
			return
		}
		fmt.Printf("Received message: %s\n", msg.Payload())
		// Send the message to the message channel for processing
		messageChan.AddContent(message.Message{Topic: msg.Topic(), Payload: msg.Payload()})
	})
	opts.OnConnectionLost = func(client MQTT.Client, err error) {
		log.Printf("Connection lost: %v", err)
	}
	opts.OnReconnecting = func(client MQTT.Client, opts *MQTT.ClientOptions) {
		log.Printf("Attempting to reconnect...")
	}

	// Create and start the client
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to broker: %v", token.Error())
	}
	defer client.Disconnect(250)
	log.Println("Connected to MQTT broker")

	// Subscribe to all topics
	if token := client.Subscribe("#", 0, nil); token.Wait() && token.Error() != nil {
		log.Fatalf("Error subscribing to topics: %v", token.Error())
	}
	log.Println("Subscribed to all topics")

	// Wait for interrupt signal
	<-sigChan
	log.Println("Interrupt signal received, shutting down...")

	// Clean up
	messageChan.CleanUp() // Close the message channel to stop workers
	wg.Wait()             // Wait for all workers to finish
	log.Println("Shutdown complete")
}

type VideoPacket struct {
	VideoID      string `json:"video_id"`
	PacketNumber int    `json:"packet_number"`
	TotalPackets int    `json:"total_packets"` // Use 0 if unknown
	Data         []byte `json:"data"`
}
