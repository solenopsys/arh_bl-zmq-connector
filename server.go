package zmq_connector

import (
	"context"
	"github.com/go-zeromq/zmq4"
	"k8s.io/klog/v2"
	"log"
	"sync"
)

type HsSever struct {
	SocketUrl string
	Streams   Streams
	socket    *zmq4.Socket
	wg        sync.WaitGroup
}

func (h *HsSever) StartServer() {
	var wg sync.WaitGroup
	wg.Add(1)
	h.startListen()

	go h.outputMessageLoop()
	go h.Streams.init()
	wg.Wait()
	klog.Info("STOP SERVER")
}

func (h *HsSever) inputMessageLoop() { //todo добавить контекст
	input := h.Streams.input()
	for {
		request, err := (*h.socket).Recv()
		if err != nil {
			log.Fatal(err)
			h.wg.Done()
		}
		input <- &SocketMassage{Body: request.Frames[1], Address: request.Frames[0]}
	}
}

func (h *HsSever) outputMessageLoop() { //todo добавить контекст
	output := h.Streams.output()
	for {
		message := <-output
		msg := zmq4.NewMsgFrom(message.Address, message.Body)
		err := (*h.socket).Send(msg)
		if err != nil {
			log.Fatalf("router failed to send message to %q: %v", message, message.Address)
		}
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (h *HsSever) startListen() {
	klog.Info("START SERVER URL ", h.SocketUrl)
	socket := zmq4.NewRouter(context.Background(), zmq4.WithID(zmq4.SocketIdentity("server")))
	err := socket.Listen(h.SocketUrl)
	if err != nil {
		klog.Errorf("could not listen %q: %v", h.SocketUrl, err)
	} else {
		klog.Info("router created and bound")
		h.socket = &socket
		go h.inputMessageLoop()
	}
}
