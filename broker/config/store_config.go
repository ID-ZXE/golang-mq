package config

import "os"

const GOLANG_MQ = "golang-mq"

const (
	COMMIT_LOG_SIZE     = 0
	CONSUME_QUEUE_SIZE  = 0
	CONSUME_OFFSET_SIZE = 0
)

var (
	commitlogPath     = ""
	consumeQueuePath  = ""
	consumeOffsetPath = ""
)

func init() {
	commitlogPath = os.Getenv("user.home") + "/" + GOLANG_MQ + "/" + "commitlog" + "/"
	consumeQueuePath = os.Getenv("user.home") + "/" + GOLANG_MQ + "/" + "consumeQueue" + "/"
	consumeOffsetPath = os.Getenv("user.home") + "/" + GOLANG_MQ + "/" + "consumeOffset" + "/"
}
