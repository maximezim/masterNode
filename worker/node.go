package worker

import (
	"sync"

	"github.com/gorilla/websocket"
)

type WorkerNode struct {
	Id   int
	Name string
	conn *websocket.Conn
	mu   sync.Mutex
}

func (worker *WorkerNode) SendMessage(message []byte) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.conn.WriteMessage(websocket.TextMessage, message)
}
