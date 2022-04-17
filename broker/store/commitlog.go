package store

import (
	"broker/store/constant"
	"github.com/common/message"
	"github.com/common/utils"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Commitlog struct {
	mappedFileQueue *MappedFileQueue //队列
	commitLogOffset int64            //所有文件总偏移量
	mu              sync.Mutex       //并发锁
}

type CommitlogPutMessageResult string

const (
	SUCCESS CommitlogPutMessageResult = "SUCCESS"
	FAILURE CommitlogPutMessageResult = "FAILURE"
)

type CommitPutMessageResult struct {
	msgSize int
	offset  int64
	result  CommitlogPutMessageResult
}

var CommitLogInstance *Commitlog

func init() {
	CommitLogInstance = &Commitlog{mappedFileQueue: NewMappedFileQueue()}
	CommitLogInstance.ensureDirExist()
	CommitLogInstance.createDefaultFile()
	CommitLogInstance.recover()
}

func (commitlog *Commitlog) recover() {
	log.Println("commit recover")
	commitlog.ensureDirExist()
	dirs, err := os.ReadDir(constant.GetFilePath(constant.Commitlog))
	if err != nil {
		panic(err)
	}

	fileNames := make([]int64, 0)
	for _, dir := range dirs {
		// 过滤文件夹与隐藏文件
		if !dir.IsDir() && strings.Contains(dir.Name(), ".") {
			fileNameInt, err := strconv.ParseInt(dir.Name(), 10, 64)
			if err != nil {
				continue
			}
			fileNames = append(fileNames, fileNameInt)
		}
	}
	// int64排序
	sort.Slice(fileNames, func(i, j int) bool {
		return fileNames[i] < fileNames[j]
	})
	for _, fileName64 := range fileNames {
		mappedFile := NewMappedFile(constant.Commitlog, strconv.FormatInt(fileName64, 10))
		commitlog.mappedFileQueue.addMappedFile(mappedFile)
	}

	if !commitlog.mappedFileQueue.isEmpty() {
		lastMappedFile := commitlog.mappedFileQueue.getLastMappedFile()
		var offset int64 = 0
		for {
			size := lastMappedFile.GetInt(offset)
			if size == 0 {
				break
			}
			msg := lastMappedFile.LoadMessage(offset, size)
			if msg == nil {
				break
			}
			offset += int64(IntLength + size)
		}
		log.Printf("commit recover, current wrote pos:%d\n", offset)
		lastMappedFile.SetWrotePos(offset)
	}
}

func (commitlog *Commitlog) ensureDirExist() {
	ensureDirExist(constant.GetFilePath(constant.Commitlog))
}

func (commitlog *Commitlog) createDefaultFile() {
	if commitlog.mappedFileQueue.isEmpty() {
		mappedFile := NewMappedFile(constant.Commitlog, "0")
		commitlog.mappedFileQueue.addMappedFile(mappedFile)
	}
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

func (commitlog *Commitlog) GetMessage(offset int64) *message.Message {
	mappedFile := commitlog.mappedFileQueue.GetMappedFileByOffset(offset)
	if mappedFile == nil {
		return nil
	}
	byteSize := mappedFile.GetInt(offset - mappedFile.fromOffset)
	if byteSize == 0 {
		return nil
	}
	pos := offset - mappedFile.fromOffset + IntLength
	return mappedFile.LoadMessage(pos, byteSize)
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
