package main

import (
	"log"
	"masterNode/loadbalancer"
	"masterNode/message"
	"masterNode/waitinglist"
	"masterNode/worker"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack"
)

// worker processes messages from the message channel
func ProcessMessageWorker(messageChan *waitinglist.WaitingList, wm *worker.WorkerManager, policyHandler *loadbalancer.PolicyHandler) {
	for {
		if messageChan.GetQueueSize() > 0 {
			msg := messageChan.GetContent()
			if msg == nil {
				break
			}

			// Check if the message is a video packet
			if strings.HasPrefix(msg.Topic, "video/stream") {
				var videoPacket VideoPacketSIS
				err := msgpack.Unmarshal(msg.Payload, &videoPacket)
				if err != nil {
					log.Printf("Error unmarshalling video packet: %v", err)
					continue
				}

				// Get a worker node based on the load-balancing policy
				workerNode := wm.GetWorkerBasedOnPolicy(policyHandler.GetPolicy())
				if workerNode == nil {
					log.Printf("No available worker nodes to send video packet")
					continue
				}

				// Serialize the video packet to JSON
				packetBytes, err := msgpack.Marshal(videoPacket)
				if err != nil {
					log.Printf("Error marshalling video packet: %v", err)
					continue
				}

				// Send the video packet to the worker node
				err = workerNode.SendMessage(packetBytes)
				if err != nil {
					log.Printf("Error sending video packet to worker %s: %v", workerNode.Name, err)
					wm.RemoveWorker(workerNode.Id)
					continue
				}

				log.Printf("Sent video packet to worker %s", workerNode.Name)
			} else {
				// Handle other message types if needed
				log.Printf("Received non-video message on topic %s", msg.Topic)
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func processMessage(msg message.Message, wm *worker.WorkerManager, ph *loadbalancer.PolicyHandler) {
	var packet VideoPacket
	err := msgpack.Unmarshal(msg.Payload, &packet)
	if err != nil {
		log.Printf("Error unmarshalling MQTT message: %v", err)
		return
	}

	// Convert packet back to JSON to send to the worker
	dataToSend, err := msgpack.Marshal(packet)
	if err != nil {
		log.Printf("Error marshalling packet to msgpack: %v", err)
		return
	}

	policy := ph.GetPolicy()

	wker := wm.GetWorkerBasedOnPolicy(policy)
	if wker == nil {
		log.Println("No worker connected or no worker matching policy")
		return
	}
	err = wker.SendMessage(dataToSend)
	if err != nil {
		log.Printf("Error sending message to worker %s: %v", wker.Name, err)
		wm.RemoveWorker(wker.Id)
	}
}

func shouldExclude(topic string) bool {
	// Exclude topics that end with "-stream", "-ping", or "-stats"
	return !strings.HasPrefix(topic, "video/stream")
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
