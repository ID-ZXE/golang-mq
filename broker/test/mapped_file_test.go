package test

import (
	"broker/store"
	"broker/store/constant"
	"fmt"
	"testing"
)

func TestMappedFileAppend(t *testing.T) {
	mappedFile := store.NewMappedFile(constant.Commitlog, "0")
	mappedFile.Append([]byte("hello go mq"))

	size := mappedFile.GetInt(0)
	fmt.Printf("size:%d\n", size)
}
