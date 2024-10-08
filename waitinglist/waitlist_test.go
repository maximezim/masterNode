package waitinglist

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestFeedAndTake(t *testing.T) {
	const iterations int = 100
	var feeder [100][]byte
	list := NewWaitingList()
	for i := range iterations {
		feeder[i] = make([]byte, 1920*1080*3)
		rand.Read(feeder[i])
		list.AddContent(feeder[i])
	}
	for i := range iterations {
		b := list.GetContent()
		if !bytes.Equal(feeder[i], *b) {
			t.Error()
			return
		}
	}
}
