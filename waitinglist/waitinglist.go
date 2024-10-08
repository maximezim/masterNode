package waitinglist

import (
	"sync"
)

type WaitingList struct {
	mut     sync.Mutex
	channel *chan *[]byte
	content int
}

func NewWaitingList() (w WaitingList) {
	w = WaitingList{
		channel: new(chan *[]byte),
	}
	(*w.channel) = make(chan *[]byte, 5000)
	return w
}

func (w *WaitingList) AddContent(b []byte) {
	w.mut.Lock()
	defer w.mut.Unlock()
	var x *[]byte
	x = new([]byte)
	*x = make([]byte, len(b))
	for i := range len(b) {
		(*x)[i] = b[i]
	}
	(*w.channel) <- x
	w.content++
}

func (w *WaitingList) GetContent() (b *[]byte) {
	w.mut.Lock()
	defer w.mut.Unlock()
	return <-*w.channel
}

func (w WaitingList) GetQueueSize() int {
	w.mut.Lock()
	defer w.mut.Unlock()
	return w.content
}
