package store

import (
	"broker/store/constant"
	"bufio"
	"github.com/common/message"
	"github.com/common/utils"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type MessageAppendResult string

const (
	OK                MessageAppendResult = "OK"                // 追加成功
	InsufficientSpace MessageAppendResult = "InsufficientSpace" // 权限不够
	IoException       MessageAppendResult = "IoException"       // io操作错误
)

const (
	IntLength  = 4
	LongLength = 8
)

type MappedFile struct {
	file       *os.File      //文件
	filePath   string        //文件路径
	fileName   string        //文件名
	writer     *bufio.Writer //操作写
	fromOffset int64         //文件起始offset
	fileSize   int64         //文件大小
	wrotePos   int64         //当前写入位置
}

func NewMappedFile(fileType constant.FileType, base string) *MappedFile {
	filePath := constant.GetFilePath(fileType) + base

	dir, _ := filepath.Split(filePath)
	ensureDirExist(dir)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	return newMappedFile(fileType, filePath, file)
}

func newMappedFile(fileType constant.FileType, filePath string, file *os.File) *MappedFile {
	mappedFile := &MappedFile{}
	mappedFile.file = file
	mappedFile.fileSize = constant.GetFileSize(fileType)
	mappedFile.fileName = file.Name()
	mappedFile.filePath = filePath
	mappedFile.writer = bufio.NewWriter(file)
	mappedFile.wrotePos = 0

	if fileType == constant.Commitlog {
		fromOffset, _ := strconv.ParseInt(mappedFile.fileName, 10, 64)
		mappedFile.fromOffset = fromOffset
	} else if fileType == constant.ConsumeQueue {
		fromOffset, _ := strconv.ParseInt(mappedFile.fileName, 10, 64)
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
			err := os.MkdirAll(dirName, 0777)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (mappedFile *MappedFile) Append(data []byte) MessageAppendResult {
	if !mappedFile.checkRemainSize(int64(len(data))) {
		return InsufficientSpace
	}

	_, _ = mappedFile.file.Seek(0, io.SeekEnd)

	_, err := mappedFile.writer.Write(data)
	if err != nil {
		return IoException
	}
	return OK
}

func (mappedFile *MappedFile) Flush() {
	_ = mappedFile.writer.Flush()
}

func (mappedFile *MappedFile) checkRemainSize(size int64) bool {
	return mappedFile.wrotePos+size <= mappedFile.fileSize
}

func (mappedFile MappedFile) GetInt(offset int64) int {
	intBytes := make([]byte, 4)
	readByNewFileHandle(mappedFile.filePath, offset, intBytes)
	return utils.BytesToInt(intBytes)
}

func (mappedFile MappedFile) UpdateInt(offset int64) (err error) {
	_, err = mappedFile.file.Seek(offset, io.SeekStart)
	if err == nil {
		int32Bytes := utils.IntToBytes(int(offset))
		_, err = mappedFile.writer.Write(int32Bytes)
		if err != nil {
			return err
		}
	}
	return err
}

func (mappedFile MappedFile) GetInt64(offset int64) int64 {
	intBytes := make([]byte, 8)
	readByNewFileHandle(mappedFile.filePath, offset, intBytes)
	return utils.BytesToInt64(intBytes)
}

func (mappedFile MappedFile) LoadMessage(offset int64, size int) *message.Message {
	fileInfo, _ := mappedFile.file.Stat()
	if offset+int64(size) > fileInfo.Size() {
		return nil
	}
	data := make([]byte, size)
	readByNewFileHandle(mappedFile.filePath, offset, data)
	return message.ToMessage(data)
}

func (mappedFile *MappedFile) GetFromOffset() int64 {
	return mappedFile.fromOffset
}

func (mappedFile *MappedFile) GetWrotePos() int64 {
	return mappedFile.wrotePos
}

func (mappedFile *MappedFile) SetWrotePos(wrotePos int64) {
	mappedFile.wrotePos = wrotePos
}

// 起一个新的文件句柄读取数据
func readByNewFileHandle(filePath string, offset int64, data []byte) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	// todo 后续再处理异常
	_, _ = file.Seek(offset, io.SeekStart)
	defer file.Close()

	reader := bufio.NewReader(file)
	_, _ = reader.Read(data)
}

// getFileName
// getLong
// updateLong
// setWrote
// getWrote
