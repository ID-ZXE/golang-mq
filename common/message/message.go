package message

import (
	"encoding/json"
)

type Message struct {
	MsgId         string
	Topic         string
	QueueId       int32
	ConsumerGroup string
	Body          string
}

func NewMessage(topic string, body string, msgId string) *Message {
	return &Message{Topic: topic, Body: body, MsgId: msgId}
}

func NewMessageWithMsgId(msgId string) *Message {
	return &Message{MsgId: msgId}
}

func (msg *Message) LoadBody(v interface{}) {
	_ = json.Unmarshal([]byte(msg.Body), v)
}

func ToMessage(data []byte) *Message {
	result := &Message{}
	_ = json.Unmarshal(data, result)
	return result
}

func (msg *Message) MessageToJsonByte() []byte {
	result, _ := json.Marshal(msg)
	return result
}
