package loadbalancer

import (
	"fmt"
	"testing"
	"time"
)

func TestMsgPack(t *testing.T) {
	a := NewPolicyHandler(":8000")
	a.AcceptLoadBalancer()
	go a.SyncPolicy()
	for {
		time.Sleep(10 * time.Second)
		fmt.Println(a.GetPolicy())
	}
}
