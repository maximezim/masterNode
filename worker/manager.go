package worker

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"
)

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

func (wm *WorkerManager) RemoveWorker(id int) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if worker, ok := wm.workers[id]; ok {
		delete(wm.workersByName, worker.Name)
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
		Id:   id,
		Name: name,
		conn: conn,
	}

	wm.workers[id] = worker
	wm.workersByName[name] = worker
	return worker
}

func WorkerWebSocketHandler(wm *WorkerManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}

		worker := wm.AddWorker(conn)
		log.Printf("Worker connected: %s", worker.Name)

		// Send assigned worker name to the worker node
		assignedNameMessage := map[string]string{"worker_name": worker.Name}
		messageBytes, err := json.Marshal(assignedNameMessage)
		if err != nil {
			log.Printf("Error marshalling assigned name: %v", err)
			return
		}
		if err := worker.conn.WriteMessage(websocket.TextMessage, messageBytes); err != nil {
			log.Printf("Error sending assigned name to worker: %v", err)
			return
		}

		go handleWorkerConnection(wm, worker)
	}
}

func handleWorkerConnection(wm *WorkerManager, worker *WorkerNode) {
	defer func() {
		wm.RemoveWorker(worker.Id)
		worker.conn.Close()
		log.Printf("Worker disconnected: %s", worker.Name)
	}()

	for {
		_, message, err := worker.conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading from %s: %v", worker.Name, err)
			break
		}
		// Handle messages from worker nodes if needed
		log.Printf("Received from %s: %s", worker.Name, message)
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
