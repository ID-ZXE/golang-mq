package message

import (
	"encoding/json"
)

type Message struct {
	MsgId         string
	Topic         string
	QueueId       int
	ConsumerGroup string
	Body          string
}

func NewMessage(topic string, queueId int, msgId string, body string) *Message {
	return &Message{Topic: topic, QueueId: queueId, MsgId: msgId, Body: body}
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
