package zmq_connector

import (
	"context"
	"os"
)

type ProcessingFunction func(message []byte, functionId uint8) []byte

type HsTemplate struct {
	Pf ProcessingFunction
}

func (ht *HsTemplate) StreamProcessor(
	stream *StreamConfig,
	cancel context.CancelFunc,
) {
	for {
		messageWr := <-stream.Input
		resBytes := ht.Pf(messageWr.Body, messageWr.Function)
		stream.Output <- &HsMassage{0, messageWr.Function, resBytes}
		//todo cancel
	}
}

func (ht *HsTemplate) Init() {
	socketUrl := os.Getenv("zmq.SocketUrl")

	streams := &StreamsHolder{
		Streams:        make(map[uint32]*StreamConfig),
		Input:          make(chan *SocketMassage, 256),
		Output:         make(chan *SocketMassage, 256),
		MessageHandler: ht.StreamProcessor,
	}

	z := &HsSever{
		SocketUrl: socketUrl,
		Streams:   streams,
	}

	z.StartServer()
}
