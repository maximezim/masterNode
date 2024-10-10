package main

import (
	"encoding/json"
	"log"
	"masterNode/loadbalancer"
	"masterNode/worker"
	"strings"
)

// worker processes messages from the message channel
func ProcessMessageWorker(messageChan <-chan Message, wm *worker.WorkerManager, ph *loadbalancer.PolicyHandler) {
	for msg := range messageChan {
		processMessage(msg, wm, ph)
	}
}

func processMessage(msg Message, wm *worker.WorkerManager, ph *loadbalancer.PolicyHandler) {
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
