package store

import (
	"github.com/common/message"
	"sync"
)

type DefaultMessageStore struct {
	commitlog    *Commitlog
	consumeQueue *ConsumeQueue
}

var defaultMessageStoreMutex = sync.Mutex{}
var defaultMessageStoreInstance *DefaultMessageStore

func GetDefaultMessageStoreInstance() *DefaultMessageStore {
	if defaultMessageStoreInstance == nil {
		defaultMessageStoreMutex.Lock()
		if defaultMessageStoreInstance == nil {
			defaultMessageStoreInstance = &DefaultMessageStore{commitlog: CommitLogInstance, consumeQueue: GetConsumeQueueInstance()}
		}
		defaultMessageStoreMutex.Unlock()
	}
	return defaultMessageStoreInstance
}

func (defaultMessageStore DefaultMessageStore) PutMessage(msg *message.Message) ConsumeQueuePutMessageResult {
	// 存储commitlog
	putMessageResult := defaultMessageStore.commitlog.PutMessage(msg)
	if putMessageResult.result == SUCCESS {
		return defaultMessageStore.consumeQueue.PutMessage(msg.Topic, msg.QueueId, putMessageResult.offset)
	}
	return ConsumeQueuePutMsgFAILURE
}

func (defaultMessageStore DefaultMessageStore) FindMessage(topic string, queueId int, group string) *message.Message {
	offset := defaultMessageStore.consumeQueue.getCommitlogOffset(topic, queueId, group)
	return defaultMessageStore.commitlog.GetMessage(offset)
}
