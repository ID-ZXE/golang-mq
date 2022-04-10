package store

import (
	"broker/store/constant"
	"broker/utils"
	"bufio"
	"os"
	"path/filepath"
	"strconv"
)

type MessageAppendResult string

const (
	OK                 = "OK"                 // 追加成功
	INSUFFICIENT_SPACE = "INSUFFICIENT_SPACE" // 权限不够
	IoException        = "IO_EXCEPTION"       // io操作错误
)

const (
	INT_LENGTH  = 4
	LONG_LENGTH = 8
)

type MappedFile struct {
	file       *os.File      //文件
	filePath   string        //文件路径
	fileName   string        //文件名
	writer     *bufio.Writer //操作写
	fromOffset int           //文件起始offset
	fileSize   int64         //文件大小
	wroteSize  int64         //当前写入位置
}

func NewMappedFile(fileType constant.FileType, fileName string) *MappedFile {
	filePath := constant.GetFilePath(fileType) + fileName

	dir, _ := filepath.Split(filePath)
	ensureDirExist(dir)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0222)
	if err != nil {
		panic(err)
	}
	return NEW2(fileType, filePath, file)
}

func NEW2(fileType constant.FileType, filePath string, file *os.File) *MappedFile {
	mappedFile := &MappedFile{}
	mappedFile.file = file
	mappedFile.fileSize = constant.GetFileSize(fileType)
	mappedFile.fileName = file.Name()
	mappedFile.filePath = filePath
	mappedFile.writer = bufio.NewWriter(file)

	if fileType == constant.Commitlog {
		fromOffset, _ := strconv.Atoi(mappedFile.fileName)
		mappedFile.fromOffset = fromOffset
	} else if fileType == constant.ConsumeQueue {
		fromOffset, _ := strconv.Atoi(mappedFile.fileName)
		mappedFile.fromOffset = fromOffset
	} else if fileType == constant.ConsumeOffset {
		mappedFile.fromOffset = 0
	} else {
		panic("FileType is error:" + fileType)
	}

	return mappedFile
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

func (mappedFile *MappedFile) Append(data []byte) MessageAppendResult {
	mappedFile.checkRemainSize(int64(len(data)))

	length := len(data)
	lengthBytes := utils.Int32ToBytes(int32(length))
	bytes := append(lengthBytes, data...)

	_, err := mappedFile.writer.Write(bytes)
	if err != nil {
		return IoException
	}
	err = mappedFile.writer.Flush()
	if err != nil {
		return IoException
	}

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
