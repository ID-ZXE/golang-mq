package store

import (
	"broker/config"
	"broker/store/constant"
	"github.com/common/utils"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type QueueIdMap map[int]*MappedFileQueue

type ConsumeQueue struct {
	mu            sync.Mutex            //并发锁
	mappedFileMap map[string]QueueIdMap //topic -> queueId -> mappedFileQueue
	consumeOffset *ConsumeOffset
}

type ConsumeQueuePutMessageResult string

const (
	ConsumeQueuePutMsgSUCCESS ConsumeQueuePutMessageResult = "SUCCESS"
	ConsumeQueuePutMsgFAILURE ConsumeQueuePutMessageResult = "FAILURE"
)

var mutex = sync.Mutex{}
var consumeQueueInstance *ConsumeQueue

func init() {
	ensureDirExist(constant.GetFilePath(constant.ConsumeQueue))
	consumeQueueInstance := GetConsumeQueueInstance()
	consumeQueueInstance.recover()
}

/**
 * consumeQueue 文件恢复
 * step1: 遍历 consumeQueue 文件夹下topic文件夹 ,如果topic文件夹为空，退出恢复流程
 * step2: 保存 topic 信息到 {@link #mappedFileMap} 中
 * step3: 遍历 topic 文件夹下queueId文件夹
 * step4: 每一个文件封装成 MappedFile 然后保存
 */
func (consumeQueue *ConsumeQueue) recover() {
	dirs, err := os.ReadDir(constant.GetFilePath(constant.ConsumeQueue))
	if err != nil {
		panic(err)
	}

	for _, dir := range dirs {
		if !dir.IsDir() || strings.Contains(dir.Name(), ".") {
			continue
		}
		consumeQueue.recoverMappedFile(dir.Name())
	}
}

func (consumeQueue *ConsumeQueue) recoverMappedFile(topic string) {
	queueDirs, err := os.ReadDir(constant.GetFilePath(constant.ConsumeQueue))
	if err != nil {
		panic(err)
	}

	for _, queueDir := range queueDirs {
		if queueDir.IsDir() || strings.Contains(queueDir.Name(), ".") {
			continue
		}

		queueId := queueDir.Name()
		queueIdVal, err := strconv.Atoi(queueId)
		if err != nil {
			continue
		}

		queueDirPath := config.JoinFilePath(constant.GetFilePath(constant.ConsumeQueue), topic, queueId)
		consumeQueueFiles, err := os.ReadDir(queueDirPath)
		if err != nil {
			panic(err)
		}

		names := make([]string, 0)
		for _, consumeQueueFile := range consumeQueueFiles {
			names = append(names, consumeQueueFile.Name())
		}
		sort.Strings(names)

		for _, name := range names {
			mappedFile := NewMappedFile(constant.ConsumeQueue, config.JoinFilePath(topic, queueId, name))
			queueIdMap := consumeQueue.mappedFileMap[topic]
			if queueIdMap == nil {
				queueIdMap = make(map[int]*MappedFileQueue, 0)
				consumeQueue.mappedFileMap[topic] = queueIdMap
			}
			mappedFileQueue := queueIdMap[queueIdVal]
			if mappedFileQueue == nil {
				mappedFileQueue = NewMappedFileQueue()
				queueIdMap[queueIdVal] = mappedFileQueue
			}
			mappedFileQueue.addMappedFile(mappedFile)
		}

		if !consumeQueue.mappedFileMap[topic][queueIdVal].isEmpty() {
			lastMappedFile := consumeQueue.mappedFileMap[topic][queueIdVal].getLastMappedFile()
			var offset int64 = 0
			for {
				size := lastMappedFile.GetInt64(0)
				if size == 0 {
					break
				}
				offset += LongLength
			}
			lastMappedFile.SetWrotePos(offset)
		}
	}
}

func GetConsumeQueueInstance() *ConsumeQueue {
	if consumeQueueInstance == nil {
		mutex.Lock()
		if consumeQueueInstance == nil {
			consumeQueueInstance = NewConsumeQueue()
			consumeQueueInstance.mkdirTopicDir()
		}
		mutex.Unlock()
	}
	return consumeQueueInstance
}

func NewConsumeQueue() *ConsumeQueue {
	return &ConsumeQueue{mappedFileMap: make(map[string]QueueIdMap, 0), consumeOffset: GetConsumeOffsetInstance()}
}

func (consumeQueue *ConsumeQueue) PutMessage(topic string, queueId int, commitlogOffset int64) ConsumeQueuePutMessageResult {
	consumeQueue.mu.Lock()
	defer consumeQueue.mu.Unlock()

	consumeQueue.ensureConsumeQueueFileExist(topic)
	mappedFileQueue := consumeQueue.getMappedFileQueue(topic, queueId)
	mappedFile := mappedFileQueue.getLastMappedFile()

	bytes := utils.Int64ToBytes(commitlogOffset)
	messageAppendResult := mappedFile.Append(bytes)

	if messageAppendResult == OK {
		mappedFile.Flush()
	} else if messageAppendResult == InsufficientSpace {
		// todo 重新构建文件
	} else {
		log.Println("append error")
	}
	return ConsumeQueuePutMsgSUCCESS
}

func (consumeQueue *ConsumeQueue) getCommitlogOffset(topic string, queueId int, group string) int64 {
	offset := consumeQueue.consumeOffset.GetOffset(topic, queueId, group)
	off := int64(offset) * LongLength
	mappedFile := consumeQueue.getMappedFileQueue(topic, queueId).GetMappedFileByOffset(off)
	return mappedFile.GetInt64(off - mappedFile.GetFromOffset())
}

func (consumeQueue *ConsumeQueue) getMappedFileQueue(topic string, queueId int) *MappedFileQueue {
	return consumeQueue.mappedFileMap[topic][queueId]
}

func (consumeQueue *ConsumeQueue) createNewFile(topic string, queueId int) {
	queueIdMap := consumeQueue.mappedFileMap[topic]
	mappedFileQueue := queueIdMap[queueId]
	lastMappedFile := mappedFileQueue.getLastMappedFile()
	fromOffset := lastMappedFile.GetFromOffset()
	wrotePos := lastMappedFile.GetWrotePos()
	nextOffset := strconv.FormatInt(fromOffset+wrotePos, 10)
	fileName := topic + "/" + nextOffset
	mappedFile := NewMappedFile(constant.ConsumeQueue, fileName)
	mappedFileQueue.addMappedFile(mappedFile)
}

func (consumeQueue *ConsumeQueue) ensureConsumeQueueFileExist(topic string) {
	queueIdMap, isExist := consumeQueue.mappedFileMap[topic]
	if isExist {
		return
	}

	queueIdMap = make(map[int]*MappedFileQueue, 0)
	topics := config.ConfigBody.Topics
	for _, topicUnit := range topics {
		if topicUnit.Topic != topic {
			continue
		}
		for i := 0; i < topicUnit.Queue; i++ {
			base := topic + "/" + strconv.Itoa(i) + "/" + "0"
			mappedFile := NewMappedFile(constant.ConsumeQueue, base)
			mappedFileQueue, exist := queueIdMap[i]
			if !exist {
				mappedFileQueue = NewMappedFileQueue()
				queueIdMap[i] = mappedFileQueue
			}
			mappedFileQueue.addMappedFile(mappedFile)
		}
	}
}

func (consumeQueue *ConsumeQueue) mkdirTopicDir() {
	consumeQueue.mkdirCustomTopicDir()
}

func (consumeQueue *ConsumeQueue) mkdirCustomTopicDir() {
	topics := config.ConfigBody.Topics
	for _, topicUnit := range topics {
		for i := 0; i < topicUnit.Queue; i++ {
			queueIdMap, exist := consumeQueue.mappedFileMap[topicUnit.Topic]
			if !exist {
				queueIdMap = make(map[int]*MappedFileQueue, 0)
				consumeQueue.mappedFileMap[topicUnit.Topic] = queueIdMap
			}
			queueIdMap[i] = NewMappedFileQueue()
			mappedFileQueue := queueIdMap[i]
			if mappedFileQueue.isEmpty() {
				topicQueuePath := topicUnit.Topic + "/" + strconv.Itoa(i) + "/"
				mappedFile := NewMappedFile(constant.ConsumeQueue, topicQueuePath+"0")
				mappedFileQueue.addMappedFile(mappedFile)
			}
		}
	}
	log.Println("topic dir init")
}
