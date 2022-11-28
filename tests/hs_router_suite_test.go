package test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHsRouter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ZmqConnector Suite")
}
