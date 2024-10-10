package main

import (
	"encoding/json"
	"log"
	"masterNode/loadbalancer"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
)

var (
	brokerURI        = "tcp://remicaulier.fr:1883"
	clientID         = "master-node"
	username         = "viewer"
	password         = "zimzimlegoat"
	excludedTopics   = []string{"worker/node/#"} // Topics to exclude
	maxWorkers       = 10                        // Maximum number of concurrent workers
	messageQueueSize = 256                       // Size of the message queue
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

	var wg sync.WaitGroup

	// Buffered channel to act as a message queue
	messageChan := make(chan Message, messageQueueSize)

	wm := NewWorkerManager()

	// Start the WebSocket server in a goroutine
	go func() {
		http.HandleFunc("/ws", workerWebSocketHandler(wm))
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	policyHandler := loadbalancer.NewPolicyHandler("0.0.0.0:8000")

	// Start accepting load balancer connections
	go func() {
		err := policyHandler.AcceptLoadBalancer()
		if err != nil {
			log.Fatalf("Failed to accept load balancer connection: %v", err)
		}
	}()

	// Start syncing policies
	go func() {
		err := policyHandler.SyncPolicy()
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
			worker(messageChan, wm, &policyHandler)
		}()
	}

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
func worker(messageChan <-chan Message, wm *WorkerManager, ph *loadbalancer.PolicyHandler) {
	for msg := range messageChan {
		processMessage(msg, wm, ph)
	}
}

func processMessage(msg Message, wm *WorkerManager, ph *loadbalancer.PolicyHandler) {
	var packet VideoPacket
	err := json.Unmarshal(msg.Payload, &packet)
	if err != nil {
		log.Printf("Error unmarshalling MQTT message: %v", err)
		return
	}

	// Convert packet back to JSON to send to the worker
	dataToSend, err := json.Marshal(packet)
	if err != nil {
		log.Printf("Error marshalling packet to JSON: %v", err)
		return
	}

	policy := ph.GetPolicy()

	worker := wm.GetWorkerBasedOnPolicy(policy)
	if worker == nil {
		log.Println("No worker connected or no worker matching policy")
		return
	}
	err = worker.SendMessage(dataToSend)
	if err != nil {
		log.Printf("Error sending message to worker %s: %v", worker.name, err)
		wm.RemoveWorker(worker.id)
	}
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

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type WorkerManager struct {
	mu              sync.Mutex
	nextID          int
	workers         map[int]*WorkerNode
	workersByName   map[string]*WorkerNode
	roundRobinIndex int
}

type WorkerNode struct {
	id   int
	name string
	conn *websocket.Conn
	mu   sync.Mutex
}

func NewWorkerManager() *WorkerManager {
	return &WorkerManager{
		workers:       make(map[int]*WorkerNode),
		workersByName: make(map[string]*WorkerNode),
		nextID:        1,
	}
}

func (wm *WorkerManager) AddWorker(conn *websocket.Conn) *WorkerNode {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	id := wm.nextID
	wm.nextID++

	name := "worker-node-" + strconv.Itoa(id)
	worker := &WorkerNode{
		id:   id,
		name: name,
		conn: conn,
	}

	wm.workers[id] = worker
	wm.workersByName[name] = worker
	return worker
}

func (wm *WorkerManager) RemoveWorker(id int) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if worker, ok := wm.workers[id]; ok {
		delete(wm.workersByName, worker.name)
		delete(wm.workers, id)
	}
}

func (wm *WorkerManager) GetNextWorker() *WorkerNode {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if len(wm.workers) == 0 {
		return nil
	}
	keys := make([]int, 0, len(wm.workers))
	for k := range wm.workers {
		keys = append(keys, k)
	}
	wm.roundRobinIndex = (wm.roundRobinIndex + 1) % len(keys)
	workerID := keys[wm.roundRobinIndex]
	return wm.workers[workerID]
}

func (worker *WorkerNode) SendMessage(message []byte) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.conn.WriteMessage(websocket.BinaryMessage, message)
}

func workerWebSocketHandler(wm *WorkerManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}

		worker := wm.AddWorker(conn)
		log.Printf("Worker connected: %s", worker.name)

		go handleWorkerConnection(wm, worker)
	}
}

func handleWorkerConnection(wm *WorkerManager, worker *WorkerNode) {
	defer func() {
		wm.RemoveWorker(worker.id)
		worker.conn.Close()
		log.Printf("Worker disconnected: %s", worker.name)
	}()

	for {
		_, message, err := worker.conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading from %s: %v", worker.name, err)
			break
		}
		// Handle messages from worker nodes if needed
		log.Printf("Received from %s: %s", worker.name, message)
	}
}

func (wm *WorkerManager) GetWorkerBasedOnPolicy(policy map[string]int) *WorkerNode {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	totalWeight := 0
	cumulativeWeights := []int{}
	workerNodes := []*WorkerNode{}

	for name, weight := range policy {
		if weight <= 0 {
			continue
		}
		worker, exists := wm.workersByName[name]
		if !exists {
			continue
		}
		totalWeight += weight
		cumulativeWeights = append(cumulativeWeights, totalWeight)
		workerNodes = append(workerNodes, worker)
	}

	if totalWeight == 0 || len(workerNodes) == 0 {
		// Fallback to round-robin
		if len(wm.workers) == 0 {
			return nil
		}
		keys := make([]int, 0, len(wm.workers))
		for k := range wm.workers {
			keys = append(keys, k)
		}
		wm.roundRobinIndex = (wm.roundRobinIndex + 1) % len(keys)
		workerID := keys[wm.roundRobinIndex]
		return wm.workers[workerID]
	}

	// Generate a random number between 0 and totalWeight - 1
	r := rand.Intn(totalWeight)

	// Select the worker based on the random number
	for i, cumWeight := range cumulativeWeights {
		if r < cumWeight {
			return workerNodes[i]
		}
	}
	return nil
}

type VideoPacket struct {
	VideoID      string `json:"video_id"`
	PacketNumber int    `json:"packet_number"`
	TotalPackets int    `json:"total_packets"` // Use 0 if unknown
	Data         []byte `json:"data"`
}
