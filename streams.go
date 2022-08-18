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
	Body    []byte
	Address []byte
}

type HsMassage struct {
	State    uint8
	Function uint8
	Body     []byte
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
	Input    chan *HsMassage
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
			header[4] = outputMessage.State
			header[5] = outputMessage.Function

			ars := [][]byte{header, outputMessage.Body}
			result := bytes.Join(ars, []byte{})

			socketOutput <- &SocketMassage{Address: sc.address, Body: result}
		}
	}
}

type StreamProcessor func(
	context context.Context,
	stream *StreamConfig,
)

type StreamsHolder struct {
	Streams        map[uint32]*StreamConfig
	Input          chan *SocketMassage
	Output         chan *SocketMassage
	MessageHandler StreamProcessor
	Meta           []byte
}

const FirstFrame = 15

func (h *StreamsHolder) InputProcessing() {
	for {
		message := <-h.Input

		var stream = binary.BigEndian.Uint32(message.Body[:4])
		var state = message.Body[4]
		var function = message.Body[5]
		var body []byte
		if state == FirstFrame {
			user := binary.BigEndian.Uint16(message.Body[6:8])
			body = message.Body[8:]
			streamConfig := &StreamConfig{
				streamId: stream,
				userId:   user,
				created:  time.Now(),
				address:  message.Address,
				Input:    make(chan *HsMassage),
				output:   make(chan *HsMassage),
			}
			h.Streams[stream] = streamConfig
			klog.Info("NEW STREAM  %d STATE %d FUNCTION  %d USER %d", stream, state, function, user)
			ctx := context.TODO()
			go h.MessageHandler(ctx, streamConfig)
			go streamConfig.outputProcessing(ctx, h.Output)

			select { //todo проверить это
			case <-ctx.Done():
				delete(h.Streams, stream)
			}
		} else {
			body = message.Body[6:]
		}

		//todo сюда не доходит
		h.Streams[stream].Input <- &HsMassage{State: state, Function: function, Body: body} //todo проверить наличие стрима
	}
}
