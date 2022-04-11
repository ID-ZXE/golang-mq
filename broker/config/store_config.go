package config

import (
	"github.com/mqcommon/constant"
	"os"
)

const GolangMq = "golang-mq"

const (
	CommitLogSize     = 8 * constant.KB
	ConsumeQueueSize  = constant.KB
	ConsumeOffsetSize = 8 * constant.B
)

var (
	CommitlogPath     = os.Getenv("HOME") + "/" + GolangMq + "/" + "commitlog" + "/"
	ConsumeQueuePath  = os.Getenv("HOME") + "/" + GolangMq + "/" + "consumeQueue" + "/"
	ConsumeOffsetPath = os.Getenv("HOME") + "/" + GolangMq + "/" + "consumeOffset" + "/"
)
