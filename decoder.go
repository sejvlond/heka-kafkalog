package heka_kafkalog

import (
	"errors"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/sejvlond/go-kafkalog/decoder"
)

type Decoder struct {
	cfg *Config
}

type Config struct {
	MsgType string `toml:"msg_type"`
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

	var (
		field  *message.Field
		fvalue interface{}
	)

	if field, err = message.NewField("key", key, ""); err != nil {
		return
	}
	pack.Message.AddField(field)

	fvalue, _ = pack.Message.GetFieldValue("file")
	file, ok := fvalue.(string)
	if !ok {
		err = errors.New("Error getting field 'file'")
		return
	}
	if field, err = message.NewField("checkpoint_file", file, ""); err != nil {
		return
	}
	pack.Message.AddField(field)

	fvalue, _ = pack.Message.GetFieldValue("offset")
	offset, ok = fvalue.(int64)
	if !ok {
		err = errors.New("Error getting field 'offset")
		return
	}
	if field, err = message.NewField("checkpoint_offset", offset,
		"count"); err != nil {
		return
	}
	pack.Message.AddField(field)

	fvalue, _ = pack.Message.GetFieldValue("hash")
	hash, ok := fvalue.(string)
	if !ok {
		err = errors.New("Error getting field 'hash'")
		return
	}
	if field, err = message.NewField("checkpoint_hash", hash, ""); err != nil {
		return
	}
	pack.Message.AddField(field)

	fvalue, _ = pack.Message.GetFieldValue("timestamp")
	timestamp, ok := fvalue.(int64)
	if !ok {
		err = errors.New("Error getting field 'timestamp'")
		return
	}
	if field, err = message.NewField("checkpoint_timestamp", timestamp,
		"count"); err != nil {
		return
	}
	pack.Message.AddField(field)

	if field, err = message.NewField("checkpoint_logger",
		pack.Message.GetLogger(), ""); err != nil {
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
