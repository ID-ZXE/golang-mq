package message

import "mqcommon/utils"

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

func (message *Message) LoadBody(v interface{}) {
	utils.JsonStringToBean(message.Body, v)
}

func ToMessage(data []byte) *Message {
	result := &Message{}
	utils.JsonBytesToBean(data, result)
	return result
}
