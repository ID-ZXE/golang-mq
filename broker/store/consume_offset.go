package store

import (
	"broker/config"
	"broker/store/constant"
	"errors"
	"github.com/common/utils"
	"log"
	"strconv"
	"sync"
)

type ConsumeOffset struct {
	consumeOffsetMap map[string]map[int]map[string]*MappedFile //topic -> queueId -> group -> mappedFile
}

var consumeOffsetInstanceMutex = sync.Mutex{}
var instance *ConsumeOffset

func init() {
	log.Println("consume offset recover")
	instance.recover()
}

func GetConsumeOffsetInstance() *ConsumeOffset {
	if instance == nil {
		consumeOffsetInstanceMutex.Lock()
		if instance == nil {
			instance = &ConsumeOffset{}
			instance.consumeOffsetMap = make(map[string]map[int]map[string]*MappedFile, 0)
		}
		consumeOffsetInstanceMutex.Unlock()
	}
	return instance
}

func (consumeOffset *ConsumeOffset) recover() {

}

func (consumeOffset *ConsumeOffset) GetOffset(topic string, queueId int, group string) int {
	consumeOffset.addConsumeGroup(topic, queueId, group, nil)
	return consumeOffset.getMappedFile(topic, queueId, group).GetInt(0)
}

func (consumeOffset *ConsumeOffset) IncrOffset(topic string, queueId int, group string) error {
	mappedFile := consumeOffset.getMappedFile(topic, queueId, group)
	if mappedFile == nil {
		return errors.New("can not find mapped file")
	}
	seq := mappedFile.GetInt(0)
	err := mappedFile.UpdateInt(int64(seq + 1))
	return err
}

func (consumeOffset *ConsumeOffset) addConsumeGroup(topic string, queueId int, group string, mappedFile *MappedFile) {
	consumeOffset.addQueue(topic, queueId)
	_, exist := consumeOffset.consumeOffsetMap[topic][queueId][group]
	if exist {
		return
	}

	if mappedFile == nil {
		mappedFile = NewMappedFile(constant.ConsumeOffset, config.JoinFilePath(topic, strconv.Itoa(queueId), group))
		// 当前不存在则写入0
		zeroBytes := utils.Int64ToBytes(0)
		mappedFile.Append(zeroBytes)
	}
	consumeOffset.consumeOffsetMap[topic][queueId][group] = mappedFile
}

func (consumeOffset *ConsumeOffset) addQueue(topic string, queueId int) {
	consumeOffset.addTopic(topic)
	_, exist := consumeOffset.consumeOffsetMap[topic][queueId]
	if exist {
		return
	}
	ensureDirExist(config.JoinFilePath(config.ConsumeQueuePath, topic, strconv.Itoa(queueId)))
	consumeOffset.consumeOffsetMap[topic][queueId] = make(map[string]*MappedFile, 0)
}

func (consumeOffset *ConsumeOffset) addTopic(topic string) {
	_, exist := consumeOffset.consumeOffsetMap[topic]
	if exist {
		return
	}
	ensureDirExist(config.JoinFilePath(config.ConsumeQueuePath, topic))
	consumeOffset.consumeOffsetMap[topic] = make(map[int]map[string]*MappedFile, 0)
}

func (consumeOffset *ConsumeOffset) getMappedFile(topic string, queueId int, group string) *MappedFile {
	return consumeOffset.consumeOffsetMap[topic][queueId][group]
}
