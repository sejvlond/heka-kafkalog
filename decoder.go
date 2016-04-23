package heka_datalog

import (
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/sejvlond/go-kafkalog/decoder"
)

type Decoder struct {
	cfg *Config
}

type Config struct {
	MsgType string `toml:"msg_type"`
	Topic   string `toml:"kafka_topic"`
}

func (this *Decoder) ConfigStruct() interface{} {
	return &Config{}
}

func (this *Decoder) Init(config interface{}) (err error) {
	this.cfg = config.(*Config)
	return
}

func (this *Decoder) Decode(pack *pipeline.PipelinePack) (
	packs []*pipeline.PipelinePack, err error) {

	value, key, offset, err := decoder.Decode(
		[]byte(pack.Message.GetPayload()))
	if err != nil {
		return
	}
	pack.Message.SetPayload(string(value))

	var field *message.Field
	if field, err = message.NewField("key", key, ""); err != nil {
		return
	}
	pack.Message.AddField(field)
	if field, err = message.NewField("offset", offset, ""); err != nil {
		return
	}
	pack.Message.AddField(field)
	if field, err = message.NewField("topic", this.cfg.Topic, ""); err != nil {
		return
	}
	pack.Message.AddField(field)
	pack.Message.SetType(this.cfg.MsgType)

	packs = []*pipeline.PipelinePack{pack}
	return
}

func init() {
	pipeline.RegisterPlugin(
		"KafkalogDecoder",
		func() interface{} { return new(Decoder) })
}
