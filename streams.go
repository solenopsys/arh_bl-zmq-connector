package zmq_connector

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"k8s.io/klog/v2"
	"time"
)

type SocketMassage struct {
	body    []byte
	address []byte
}

type HsMassage struct {
	state    uint8
	function uint8
	body     []byte
}

type Streams interface {
	input() chan *SocketMassage
	output() chan *SocketMassage
}

type StreamConfig struct {
	userId   uint16
	streamId uint32
	created  time.Time
	address  []byte
	input    chan *HsMassage
	output   chan *HsMassage
}

func (sc StreamConfig) outputProcessing(ctx context.Context, socketOutput chan *SocketMassage) {
	for {

		select {
		case <-ctx.Done():
			fmt.Println("Exiting from reading go routine")
			return
		case outputMessage := <-sc.output:
			header := make([]byte, 6)

			binary.BigEndian.PutUint32(header[0:4], sc.streamId)
			header[4] = outputMessage.state
			header[5] = outputMessage.function

			ars := [][]byte{header, outputMessage.body}
			result := bytes.Join(ars, []byte{})

			socketOutput <- &SocketMassage{address: sc.address, body: result}
		}
	}
}

type StreamProcessor func(
	context context.Context,
	stream *StreamConfig,
)

type StreamsHolder struct {
	streams        map[uint32]*StreamConfig
	input          chan *SocketMassage
	output         chan *SocketMassage
	messageHandler StreamProcessor
	meta           []byte
}

const FirstFrame = 15

func (h *StreamsHolder) inputProcessing() {
	for {
		message := <-h.input

		var stream = binary.BigEndian.Uint32(message.body[:4])
		var state = message.body[4]
		var function = message.body[5]
		var body []byte
		if state == FirstFrame {
			user := binary.BigEndian.Uint16(message.body[6:8])
			body = message.body[8:]
			streamConfig := &StreamConfig{
				streamId: stream,
				userId:   user,
				created:  time.Now(),
				address:  message.address,
				input:    make(chan *HsMassage),
				output:   make(chan *HsMassage),
			}
			h.streams[stream] = streamConfig
			klog.Info("NEW STREAM  %d STATE %d FUNCTION  %d USER %d", stream, state, function, user)
			ctx := context.TODO()
			go h.messageHandler(ctx, streamConfig)
			go streamConfig.outputProcessing(ctx, h.output)

			select { //todo проверить это
			case <-ctx.Done():
				delete(h.streams, stream)
			}
		} else {
			body = message.body[6:]
		}

		h.streams[stream].input <- &HsMassage{state: state, function: function, body: body} //todo проверить наличие стрима
	}
}
