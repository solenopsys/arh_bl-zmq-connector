package test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"solenopsys.org/zmq_connector"
	"time"
)

var _ = Describe("HsServer", func() {

	var holder *zmq_connector.StreamsHolder
	var lastMessage *zmq_connector.HsMassage

	var mockHandler = func(context context.Context, stream *zmq_connector.StreamConfig) {
		go func() {
			for {
				lastMessage = <-stream.Input
			}

		}()

	}

	BeforeEach(func() {
		holder = &zmq_connector.StreamsHolder{
			Streams:        make(map[uint32]*zmq_connector.StreamConfig),
			Input:          make(chan *zmq_connector.SocketMassage),
			Output:         make(chan *zmq_connector.SocketMassage),
			MessageHandler: mockHandler,
			Meta:           []byte{},
		}
		go holder.InputProcessing()
	})

	Describe("Parsing tests", func() {
		Context("Test message", func() {
			It("should be true", func() {
				holder.Input <- &zmq_connector.SocketMassage{
					Body:    []byte{0, 0, 0, 34, 15, 4, 0, 10, 32, 34},
					Address: []byte{10, 20},
				}

				holder.Input <- &zmq_connector.SocketMassage{
					Body:    []byte{0, 0, 0, 34, 15, 4, 0, 10, 32, 34},
					Address: []byte{10, 20},
				}

				item := &zmq_connector.HsMassage{4, 0, []byte{32, 34}}
				time.Sleep(1000 * time.Millisecond)
				Expect(lastMessage).To(Equal(item))
			})

		})
	})

})
