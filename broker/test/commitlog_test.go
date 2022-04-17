package test

import (
	"broker/store"
	"broker/store/constant"
	"fmt"
	"github.com/common/message"
	"github.com/common/utils"
	"log"
	"testing"
)

/**
测试往commitlog文件写入Message对象
*/
func TestCommitlogPutMessage(t *testing.T) {
	msg := message.NewMessage("topic", 1, "100", "message body")
	store.CommitLogInstance.PutMessage(msg)
}

/**
测试从commitlog中读取Message对象
*/
func TestLoadCommitlogMessage(t *testing.T) {
	mappedFile := store.NewMappedFile(constant.Commitlog, "0")
	msg := mappedFile.LoadMessage(0+store.IntLength, 86)
	fmt.Printf("msg:%s\n", utils.ToJsonString(msg))
}

/**
测试根据offset获取Message对象
*/
func TestGetMessage(t *testing.T) {
	msg := store.CommitLogInstance.GetMessage(0)
	log.Printf("msg:%s\n", utils.ToJsonString(msg))
}

func TestCommitlogRecover(t *testing.T) {
	instance := store.CommitLogInstance
	log.Println("recover")
	msg := instance.GetMessage(0)
	log.Printf("msg:%s\n", utils.ToJsonString(msg))
}
