package test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"solenopsys.org/zmq_connector"
	"time"
)

func timeout() {
	time.Sleep(100 * time.Millisecond)
}

var _ = Describe("HsServer", func() {

	var holder *zmq_connector.StreamsHolder
	var lastMessageInput *zmq_connector.HsMassage
	var lastMessageOutput *zmq_connector.HsMassage

	var mockHandler = func(stream *zmq_connector.StreamConfig, cansel context.CancelFunc) {

		go func() {
			for {
				lastMessageInput = <-stream.Input

				//stream.Output =<-
			}

		}()

		go func() {
			for {
				lastMessageOutput = <-stream.Output

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
				timeout()
				Expect(lastMessageInput).To(Equal(&zmq_connector.HsMassage{15, 4, []byte{32, 34}}))
				holder.Input <- &zmq_connector.SocketMassage{
					Body:    []byte{0, 0, 0, 34, 0, 4, 0, 10, 32, 34},
					Address: []byte{10, 20},
				}
				timeout()
				Expect(lastMessageInput).To(Equal(&zmq_connector.HsMassage{0, 4, []byte{0, 10, 32, 34}}))
			})
			It("should be error", func() {
				holder.Input <- &zmq_connector.SocketMassage{
					Body:    []byte{0, 0, 0, 34, 0, 4, 0, 10, 32, 34},
					Address: []byte{10, 20},
				}
				timeout()
				Expect(lastMessageOutput).To(Equal(&zmq_connector.HsMassage{13, 4, []byte("StreamNotOpen")}))
			})
		})
	})

})
