package zmq_connector

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/go-zeromq/zmq4"
	"k8s.io/klog/v2"
	"log"
	"os"
	"sync"
)

func router(url string) *zmq4.Socket {
	log.Println("URL ", url)
	// Create a router socket and bind it to port 5555.
	router := zmq4.NewRouter(context.Background(), zmq4.WithID(zmq4.SocketIdentity("router")))
	err := router.Listen(url)
	if err != nil {
		log.Fatalf("could not listen %q: %v", url, err)
	}
	//defer router.Close()

	klog.Info("router created and bound")
	return &router
}

type MessageHandler func(message []byte, streamId uint32, serviceId uint16, functionId uint16) []byte

func procFunc(router *zmq4.Socket, messageHandler MessageHandler, message []byte, fromAddress []byte) {
	var header []byte = message[:8]
	body := message[8:]

	var streamId uint32 = binary.BigEndian.Uint32(header[:4])
	var serviceId uint16 = binary.BigEndian.Uint16(header[4:6])
	var functionId uint16 = binary.BigEndian.Uint16(header[6:8])

	klog.Info("STREAM  %d SERVICE %d FUNCTION  %d", streamId, serviceId, functionId)

	resultBody := messageHandler(body, streamId, serviceId, functionId)
	ars := [][]byte{header, resultBody}
	result := bytes.Join(ars, []byte{})

	msg := zmq4.NewMsgFrom(fromAddress, result)
	err := (*router).Send(msg)
	if err != nil {
		log.Fatalf("router failed to send message to %q: %v", message, fromAddress)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func processMessageLoop(router *zmq4.Socket, messageHandler MessageHandler, wg sync.WaitGroup) {
	for true {
		request, err := (*router).Recv()
		if err != nil {
			log.Fatal(err)
			wg.Done()
		}

		address := request.Frames[0]
		message := request.Frames[1]
		go procFunc(router, messageHandler, message, address)
	}
}

func StartServer(messageHandler MessageHandler) {
	var wg sync.WaitGroup
	wg.Add(1)
	log.SetOutput(os.Stdout)
	sock := router(os.Getenv("zmq.SocketUrl"))
	go processMessageLoop(sock, messageHandler, wg)
	klog.Info("START SERVER")
	wg.Wait()
	klog.Info("STOP SERVER")
}
