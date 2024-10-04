package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var (
	brokerURI        = "tcp://remicaulier.fr:1883"
	clientID         = "master-node"
	username         = "viewer"
	password         = "zimzimlegoat"
	excludedTopics   = []string{"worker/node/#"} // Topics to exclude
	maxWorkers       = 10                        // Maximum number of concurrent workers
	messageQueueSize = 100                       // Size of the message queue
)

// Message represents an MQTT message
type Message struct {
	Topic   string
	Payload []byte
}

func main() {
	// Create a channel to capture interrupt signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Buffered channel to act as a message queue
	messageChan := make(chan Message, messageQueueSize)

	// Start worker pool
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(messageChan, workerID)
		}(i)
	}

	// MQTT client options
	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerURI)
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		// Filter out messages from excluded topics
		if shouldExclude(msg.Topic()) {
			return
		}
		// Send the message to the message channel for processing
		messageChan <- Message{Topic: msg.Topic(), Payload: msg.Payload()}
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
	close(messageChan) // Close the message channel to stop workers
	wg.Wait()          // Wait for all workers to finish
	log.Println("Shutdown complete")
}

// worker processes messages from the message channel
func worker(messageChan <-chan Message, workerID int) {
	for msg := range messageChan {
		processMessage(msg, workerID)
	}
}

// processMessage handles the message processing logic
func processMessage(msg Message, workerID int) {
	republishTopic := determineRepublishTopic(msg)

	// TODO: Republish the message to the new topic
	// For example:
	// publishMessage(republishTopic, msg.Payload)
}

func determineRepublishTopic(msg Message) string {
	// TODO: determine the republish topic
	return "republish/" + msg.Topic
}

func shouldExclude(topic string) bool {
	for _, pattern := range excludedTopics {
		if topicMatches(pattern, topic) {
			return true
		}
	}
	return false
}

// topicMatches checks if a topic matches a pattern with wildcards
func topicMatches(pattern, topic string) bool {
	patternParts := strings.Split(pattern, "/")
	topicParts := strings.Split(topic, "/")

	for i, part := range patternParts {
		if part == "#" {
			return true // Wildcard matches any remaining parts
		}
		if i >= len(topicParts) || (part != topicParts[i] && part != "+") {
			return false
		}
	}
	return len(patternParts) == len(topicParts)
}

func publishMessage(topic string, payload []byte) {
	// TODO: Implement the publishing logic
	fmt.Printf("Publishing to topic %s: %s\n", topic, string(payload))
}
