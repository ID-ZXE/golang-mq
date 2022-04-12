package store

import (
	"broker/store/constant"
	"github.com/common/message"
	"github.com/common/utils"
	"log"
	"os"
	"sync"
)

type Commitlog struct {
	mappedFileQueue *MappedFileQueue //队列
	commitLogOffset int64            //所有文件总偏移量
	mu              sync.Mutex       //并发锁
}

type PutMessageResult string

const (
	SUCCESS PutMessageResult = "SUCCESS"
	FAILURE PutMessageResult = "FAILURE"
)

type CommitPutMessageResult struct {
	msgSize int
	offset  int64
	result  PutMessageResult
}

var CommitLogInstance *Commitlog

func init() {
	CommitLogInstance = &Commitlog{mappedFileQueue: NewMappedFileQueue()}
	CommitLogInstance.ensureDirExist()
	CommitLogInstance.createDefaultFile()
}

func (commitlog *Commitlog) ensureDirExist() {
	_, err := os.Stat(constant.GetFilePath(constant.Commitlog))
	if os.IsNotExist(err) {
		err := os.MkdirAll(constant.GetFilePath(constant.Commitlog), 0777)
		if err != nil {
			panic(err)
		}
	}
}

func (commitlog *Commitlog) createDefaultFile() {
	if commitlog.mappedFileQueue.isEmpty() {
		mappedFile := NewMappedFile(constant.Commitlog, "0")
		commitlog.mappedFileQueue.addMappedFile(mappedFile)
	}
}

func (commitlog *Commitlog) Recover() {

}

func (commitlog *Commitlog) PutMessage(msg *message.Message) *CommitPutMessageResult {
	commitlog.mu.Lock()
	defer commitlog.mu.Unlock()
	log.Println("put message")

	fileWritePos := commitlog.mappedFileQueue.getLastMappedFile().GetFromOffset() + commitlog.mappedFileQueue.getLastMappedFile().GetWrotePos()
	msgBytes := msg.MessageToJsonByte()
	log.Printf("put message bytes len:%d\n", len(msgBytes))
	lenBytes := utils.IntToBytes(len(msgBytes))
	data := append(lenBytes, msgBytes...)
	messageAppendResult := commitlog.mappedFileQueue.getLastMappedFile().Append(data)
	if messageAppendResult == OK {
		commitlog.mappedFileQueue.getLastMappedFile().Flush()
	} else if messageAppendResult == InsufficientSpace {
		// todo 重新构建文件
	} else {
		log.Println("append error")
	}
	commitlog.commitLogOffset += int64(len(data))
	return successPutMessageResult(fileWritePos, len(msgBytes))
}

func successPutMessageResult(offset int64, msgSize int) *CommitPutMessageResult {
	return &CommitPutMessageResult{
		result:  SUCCESS,
		offset:  offset,
		msgSize: msgSize,
	}
}

func failurePutMessageResult() *CommitPutMessageResult {
	return &CommitPutMessageResult{
		result:  FAILURE,
		offset:  -1,
		msgSize: -1,
	}
}
