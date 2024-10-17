package waitinglist

import (
	"masterNode/message"
	"sync"
)

type WaitingList struct {
	mut     sync.Mutex
	channel *chan *message.Message
	content int
}

func NewWaitingList(buff_size int) (w WaitingList) {
	w = WaitingList{
		channel: new(chan *message.Message),
	}
	(*w.channel) = make(chan *message.Message, buff_size)
	return w
}

func (w *WaitingList) AddContent(b message.Message) {
	w.mut.Lock()
	defer w.mut.Unlock()
	var x *message.Message
	x = new(message.Message)
	*x = message.Message{
		Topic:   b.Topic,
		Payload: make([]byte, len(b.Payload)),
	}
	for i := range len(b.Payload) {
		x.Payload[i] = b.Payload[i]
	}
	(*w.channel) <- x
	w.content++
}

func (w *WaitingList) GetContent() (b *message.Message) {
	w.mut.Lock()
	defer w.mut.Unlock()
	w.content--
	return <-*w.channel
}

func (w *WaitingList) GetQueueSize() int {
	w.mut.Lock()
	defer w.mut.Unlock()
	return w.content
}

func (w *WaitingList) CleanUp() {
	close(*(w.channel))
	w.channel = nil
}
