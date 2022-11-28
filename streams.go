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
	init()
}

type StreamConfig struct {
	userId   uint16
	streamId uint32
	created  time.Time
	address  []byte
	Input    chan *HsMassage
	Output   chan *HsMassage
}

func (sc StreamConfig) outputProcessing(ctx context.Context, socketOutput chan *SocketMassage) {
	for {

		select {
		case <-ctx.Done():
			fmt.Println("Exiting from reading go routine")
			return
		case outputMessage := <-sc.Output:
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
	stream *StreamConfig,
	cancel context.CancelFunc,
)

type StreamsHolder struct {
	Streams        map[uint32]*StreamConfig
	Input          chan *SocketMassage
	Output         chan *SocketMassage
	MessageHandler StreamProcessor
	Meta           []byte
}

func (h *StreamsHolder) input() chan *SocketMassage {
	return h.Input
}

func (h *StreamsHolder) output() chan *SocketMassage {
	return h.Output
}

func (h *StreamsHolder) init() {
	ctx := context.Background()
	go h.InputProcessing(ctx)
}
func (h *StreamsHolder) ErrorResponse(stream uint32, function uint8, Address []byte, errorCode ErrorCodes) {
	header := make([]byte, 6)
	binary.BigEndian.PutUint32(header[0:4], stream)
	header[4] = StreamError
	header[5] = function
	body := append(header, errorCode.String()...)
	h.Output <- &SocketMassage{body, Address}
}

func (h *StreamsHolder) InputProcessing(ctx context.Context) {
	for {

		select {

		case message := <-h.Input:
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
					Output:   make(chan *HsMassage),
				}
				h.Streams[stream] = streamConfig
				klog.Info("NEW STREAM  %d STATE %d FUNCTION  %d USER %d", stream, state, function, user)
				subCtx, cancelFunc := context.WithCancel(context.Background())
				go h.MessageHandler(
					streamConfig,
					cancelFunc,
				)
				go streamConfig.outputProcessing(subCtx, h.Output)
			} else {
				body = message.Body[6:]
			}

			if streamConf, has := h.Streams[stream]; has {
				streamConf.Input <- &HsMassage{State: state, Function: function, Body: body} //todo проверить наличие стрима
			} else {
				h.ErrorResponse(stream, function, message.Address, EndpointStreamNotFound)
			}

		case <-ctx.Done():
			break
		}

	}
}
