package loadbalancer

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

type PolicyHandler struct {
	P   map[string]int
	srv net.Listener
	cli net.Conn
	sync.RWMutex
}

func NewPolicyHandler(addrss string) PolicyHandler {
	srv, err := net.Listen("tcp", addrss)
	if err != nil {
		panic(err)
	}
	return PolicyHandler{
		P:   make(map[string]int),
		srv: srv,
	}
}

func (p *PolicyHandler) AcceptLoadBalancer() error {
	var err error
	p.Lock()
	p.cli, err = p.srv.Accept()
	p.Unlock()
	if err != nil {
		return err
	}
	return nil
}

/**
* NEED TO BE LAUNCHED IN GOROUTINE
 */
func (p *PolicyHandler) SyncPolicy() error {
	for {
		err := p.fetch_policy()
		if err != nil {
			return err
		}
	}
}

func (p *PolicyHandler) GetPolicy() map[string]int {
	p.RLock()
	defer p.RUnlock()
	return p.P
}

func (p *PolicyHandler) fetch_policy() error {
	p.RLock()
	if p.cli == nil {
		p.RUnlock()
		return errors.New("No lb connected.")
	}
	p.RUnlock()
	lenbuf := make([]byte, 4)
	n, err := p.cli.Read(lenbuf)
	if err != nil {
		return err
	}
	if n != 4 {
		return errors.New("Can't read length")
	}
	var length uint32
	length = binary.BigEndian.Uint32(lenbuf)
	// println(length)
	// fmt.Println(lenbuf)
	buff := make([]byte, length)
	n, err = p.cli.Read(buff)
	if err != nil {
		return err
	}
	if n != int(length) {
		return errors.New("Can't read message")
	}
	// fmt.Printf("%v => %s\n", buff, buff)
	p.Lock()
	msgpack.Unmarshal(buff, &p.P)
	p.Unlock()
	// fmt.Println(p.P)
	return nil
}
