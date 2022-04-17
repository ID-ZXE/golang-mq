package store

import "log"

type MappedFileQueue struct {
	queue []*MappedFile
}

func NewMappedFileQueue() *MappedFileQueue {
	return &MappedFileQueue{queue: make([]*MappedFile, 0)}
}

func (mappedFileQueue *MappedFileQueue) addMappedFile(mappedFile *MappedFile) {
	mappedFileQueue.queue = append(mappedFileQueue.queue, mappedFile)
}

func (mappedFileQueue *MappedFileQueue) getLastMappedFile() *MappedFile {
	return mappedFileQueue.queue[len(mappedFileQueue.queue)-1]
}

func (mappedFileQueue *MappedFileQueue) GetMappedFileByOffset(offset int64) *MappedFile {
	for i := mappedFileQueue.size() - 1; i >= 0; i-- {
		mappedFile := mappedFileQueue.getByIndex(i)
		if mappedFile.fromOffset <= offset {
			return mappedFile
		}
	}
	log.Printf("find mapped file failure, offset:%d\n", offset)
	return nil
}

func (mappedFileQueue *MappedFileQueue) getByIndex(index int) *MappedFile {
	return mappedFileQueue.queue[index]
}

func (mappedFileQueue *MappedFileQueue) isEmpty() bool {
	return len(mappedFileQueue.queue) == 0
}

func (mappedFileQueue *MappedFileQueue) size() int {
	return len(mappedFileQueue.queue)
}
