package constant

import (
	"broker/config"
	"fmt"
)

type FileType string

const (
	Commitlog     FileType = "Commitlog"
	ConsumeQueue  FileType = "ConsumeQueue"
	ConsumeOffset FileType = "ConsumeOffset"
)

func GetFilePath(fileType FileType) string {
	if fileType == Commitlog {
		return config.CommitlogPath
	} else if fileType == ConsumeQueue {
		return config.ConsumeQueuePath
	} else if fileType == ConsumeOffset {
		return config.ConsumeOffsetPath
	}
	panic(fmt.Sprintf("fileType %s not exist", fileType))
}

func GetFileSize(fileType FileType) int64 {
	if fileType == Commitlog {
		return config.CommitLogSize
	} else if fileType == ConsumeQueue {
		return config.ConsumeQueueSize
	} else if fileType == ConsumeOffset {
		return config.ConsumeOffsetSize
	}
	panic(fmt.Sprintf("fileType %s not exist", fileType))
}
