package heka_datalog

import (
	"github.com/mozilla-services/heka/pipeline"
	"github.com/sejvlond/go-kafkalog/common"
)

type Splitter struct{}

func (this *Splitter) Init(config interface{}) (err error) {
	return
}

func (this *Splitter) FindRecord(buf []byte) (
	bytesRead int, record []byte) {

	var (
		msgLen int
		expLen int
		err    error
	)
	if _, msgLen, err = common.ParseMessageSetHeader(buf); err != nil {
		return
	}
	if expLen = common.MSGSET_HEADER_SIZE + msgLen; len(buf) < expLen {
		return
	}
	return expLen, buf[:expLen]
}

func init() {
	pipeline.RegisterPlugin(
		"KafkalogSplitter",
		func() interface{} { return new(Splitter) })
}
