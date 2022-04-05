package config

import "os"

const TOY_MQ = "toy-mq"

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
	commitlogPath = os.Getenv("user.home") + "/" + TOY_MQ + "/" + "commitlog" + "/"
	consumeQueuePath = os.Getenv("user.home") + "/" + TOY_MQ + "/" + "consumeQueue" + "/"
	consumeOffsetPath = os.Getenv("user.home") + "/" + TOY_MQ + "/" + "consumeOffset" + "/"
}
