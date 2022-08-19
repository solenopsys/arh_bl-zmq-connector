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

	var mockHandler = func(stream *zmq_connector.StreamConfig, cansel context.CancelFunc) {

		go func() {
			for {
				lastMessage = <-stream.Input

				//stream.Output =<-
			}

		}()

	}

	BeforeEach(func() {
		holder = &zmq_connector.StreamsHolder{
			Streams:        make(map[uint32]*zmq_connector.StreamConfig),
			Input:          make(chan *zmq_connector.SocketMassage, 256),
			Output:         make(chan *zmq_connector.SocketMassage, 256),
			MessageHandler: mockHandler,
			Meta:           []byte{},
		}
		ctx := context.TODO()
		go holder.InputProcessing(ctx)
	})

	Describe("Parsing tests", func() {
		Context("Test create stream", func() {
			It("should be message equal", func() {
				holder.Input <- &zmq_connector.SocketMassage{
					Body:    []byte{0, 0, 0, 34, 15, 4, 0, 10, 32, 34},
					Address: []byte{10, 20},
				}
				time.Sleep(100 * time.Millisecond)
				Expect(lastMessage).To(Equal(&zmq_connector.HsMassage{15, 4, []byte{32, 34}}))
				holder.Input <- &zmq_connector.SocketMassage{
					Body:    []byte{0, 0, 0, 34, 0, 4, 0, 10, 32, 34},
					Address: []byte{10, 20},
				}
				time.Sleep(100 * time.Millisecond)
				Expect(lastMessage).To(Equal(&zmq_connector.HsMassage{0, 4, []byte{0, 10, 32, 34}}))

			})

		})
	})

})
