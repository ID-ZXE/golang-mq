package test

import (
	"broker/store"
	"broker/store/constant"
	"fmt"
	"github.com/common/message"
	"github.com/common/utils"
	"testing"
)

func TestPutMessage(t *testing.T) {
	msg := message.NewMessage("topic", "body", "100")
	store.CommitLogInstance.PutMessage(msg)
}

func TestLoadMessage(t *testing.T) {
	mappedFile := store.NewMappedFile(constant.Commitlog, "0")
	msg := mappedFile.LoadMessage(0+store.IntLength, 76)
	fmt.Printf("msg:%s\n", utils.ToJsonString(msg))
}
