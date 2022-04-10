package test

import (
	"broker/store"
	"broker/store/constant"
	"testing"
)

func TestMappedFile(t *testing.T) {
	mappedFile := store.NewMappedFile(constant.Commitlog, "0")
	mappedFile.Append([]byte("hello go mq"))
}
