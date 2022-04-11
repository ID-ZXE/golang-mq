package test

import (
	"broker/store"
	"broker/store/constant"
	"fmt"
	"testing"
)

func TestMappedFileAppend(t *testing.T) {
	mappedFile := store.NewMappedFile(constant.Commitlog, "0")
	data := []byte("hello go mq")
	mappedFile.Append(data)

	size := mappedFile.GetInt(0)
	fmt.Printf("size:%d\n", size)
	fmt.Printf("size:%d\n", len(data))
}

func TestLoadMessage(t *testing.T) {
	//mappedFile := store.NewMappedFile(constant.Commitlog, "0")
	//message := mappedFile.LoadMessage(0, 0)
	//fmt.Printf("msg:%s\b", utils.ToJsonString(message))
}
