package store

import (
	"broker/store/constant"
	"os"
	"path/filepath"
	"strconv"
)

type MessageAppendResult string

const (
	OK                 = "OK"                 // 追加成功
	INSUFFICIENT_SPACE = "INSUFFICIENT_SPACE" // 权限不够
	IO_EXCEPTION       = "IO_EXCEPTION"       // io操作错误
)

const (
	INT_LENGTH  = 4
	LONG_LENGTH = 8
)

type MappedFile struct {
	file       *os.File //文件
	filePath   string   //文件路径
	fileName   string   //文件名
	fromOffset int      //文件起始offset
	fileSize   int64    //文件大小
	wroteSize  int64    //当前写入位置
}

func New(fileType store.FileType, fileName string) *MappedFile {
	filePath := store.GetFilePath(fileType) + fileName
	file, err := os.OpenFile(filePath, os.O_RDWR, 0222)
	if err == nil {
		panic(err)
	}
	return NEW2(fileType, filePath, file)
}

func NEW2(fileType store.FileType, filePath string, file *os.File) *MappedFile {
	mappedFile := &MappedFile{}
	mappedFile.file = file
	mappedFile.fileSize = store.GetFileSize(fileType)
	mappedFile.fileName = file.Name()
	mappedFile.filePath = filePath

	if fileType == store.COMMITLOG {
		fromOffset, _ := strconv.Atoi(mappedFile.fileName)
		mappedFile.fromOffset = fromOffset
	} else if fileType == store.CONSUME_QUEUE {
		fromOffset, _ := strconv.Atoi(mappedFile.fileName)
		mappedFile.fromOffset = fromOffset
	} else if fileType == store.CONSUME_OFFSET {
		mappedFile.fromOffset = 0
	} else {
		panic("FileType is error:" + fileType)
	}
	fieldsInit(mappedFile)
	return mappedFile
}

func fieldsInit(mappedFile *MappedFile) {
	dir, _ := filepath.Split(mappedFile.filePath)
	ensureDirExist(dir)
}

func ensureDirExist(dirName string) {
	if len(dirName) != 0 {
		_, err := os.Stat(dirName)
		if os.IsNotExist(err) {
			err := os.MkdirAll(dirName, 0222)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (mappedFile *MappedFile) append(data []byte) MessageAppendResult {
	mappedFile.checkRemainSize(int64(len(data)))
	return OK
}

func (mappedFile *MappedFile) checkRemainSize(size int64) bool {
	return mappedFile.wroteSize+size <= mappedFile.fileSize
}

// getFileName
// loadMessage
// getInt
// updateInt
// getLong
// updateLong
// setWrote
// getWrote
