package zmq_connector

const FirstFrame = 15
const StreamError = 13

type ErrorCodes uint8

const (
	EndpointStreamNotFound ErrorCodes = iota
)

func (r ErrorCodes) String() string {
	return [...]string{
		"EndpointStreamNotFound",
	}[r]
}
