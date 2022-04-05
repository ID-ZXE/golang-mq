package store

import (
	"broker/store/constant"
	"os"
)

const (
	INT_LENGTH  = 4
	LONG_LENGTH = 8
)

type MappedFile struct {
	file       *os.File // 文件
	filePath   string   // 文件路径
	fileName   string   //文件名
	fromOffset int32    // 文件起始offset
	fileSize   int32    //文件大小
}

func New(fileType store.FileType, fileName string) *MappedFile {
	file, err := os.Create(store.GetFilePath(fileType) + fileName)
	if err == nil {
		panic(err)
	}
	return Default(fileType, file)
}

func Default(fileType store.FileType, file *os.File) *MappedFile {
	mappedFile := &MappedFile{}
	mappedFile.file = file
	mappedFile.fileName = file.Name()

	if fileType == store.COMMITLOG {

	} else if fileType == store.CONSUME_QUEUE {

	} else if fileType == store.CONSUME_OFFSET {

	} else {
		panic("FileType is error:" + fileType)
	}
	fieldsInit(mappedFile)
	return mappedFile
}

func fieldsInit(mappedFile *MappedFile) {

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
