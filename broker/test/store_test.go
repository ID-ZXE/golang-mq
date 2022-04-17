package test

import (
	"broker/config"
	"broker/store"
	"broker/store/constant"
	"github.com/common/message"
	"github.com/common/utils"
	"log"
	"testing"
)

func TestJoinFilePath(t *testing.T) {
	log.Println(config.JoinFilePath("a", "b", "c"))
}

/**
测试加载配置文件
*/
func TestLoadConfig(t *testing.T) {
	brokerConfigFile := config.NewBrokerConfigFileAndLoad()
	log.Printf("%v\n", brokerConfigFile)
}

/**
测试往consumeQueue文件中写入offset
*/
func TestConsumeQueuePutMessage(t *testing.T) {
	instance := store.GetConsumeQueueInstance()
	instance.PutMessage("topic_1", 0, 169)
}

/**
测试往consumeQueue文件中读取offset
*/
func TestLoadConsumeQueueMessage(t *testing.T) {
	mappedFile := store.NewMappedFile(constant.ConsumeQueue, "/topic_1/1/0")
	val := mappedFile.GetInt64(0)
	log.Printf("offset:%d\n", val)
}

func TestDefaultMessagePutMessage(t *testing.T) {
	// len 86
	msg := message.NewMessage("topic_1", 1, "100", "message body")
	defaultMessageStoreInstance := store.GetDefaultMessageStoreInstance()
	defaultMessageStoreInstance.PutMessage(msg)
}

func TestFindMessage(t *testing.T) {
	instance := store.GetDefaultMessageStoreInstance()
	msg := instance.FindMessage("topic_1", 1, "test_group")
	log.Printf("msg:%s\n", utils.ToJsonString(msg))
}
