package config

import (
	"github.com/common/constant"
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
	BrokerConfigPath  = os.Getenv("HOME") + "/" + GolangMq + "/" + "config/masterConfig"
	StoreConfigPath   = os.Getenv("HOME") + "/" + GolangMq + "/" + "config/masterStoreConfig"
)

func JoinFilePath(paths ...string) string {
	var result = ""
	for _, path := range paths {
		result += "/" + path
	}
	return result
}
