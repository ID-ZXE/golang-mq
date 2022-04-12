package store

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

func (mappedFileQueue *MappedFileQueue) isEmpty() bool {
	return len(mappedFileQueue.queue) == 0
}
