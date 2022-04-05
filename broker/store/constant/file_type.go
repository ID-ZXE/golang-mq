package store

type FileType string

const (
	COMMITLOG      FileType = "COMMITLOG"
	CONSUME_QUEUE  FileType = "CONSUME_QUEUE"
	CONSUME_OFFSET FileType = "CONSUME_OFFSET"
)

func GetFilePath(fileType FileType) string {
	return "/"
}

func GetFileSize(fileType FileType) int64 {
	return 0
}
