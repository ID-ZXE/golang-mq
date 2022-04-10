package utils

import (
	"bytes"
	"encoding/binary"
)

func Int32ToBytes(n int32) []byte {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, n)
	return buffer.Bytes()
}

func BytesToInt32(bys []byte) int32 {
	buffer := bytes.NewBuffer(bys)
	var data int32
	_ = binary.Read(buffer, binary.BigEndian, &data)
	return data
}

func Int64ToBytes(n int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, n)
	return buffer.Bytes()
}

func BytesToInt64(bys []byte) int64 {
	buffer := bytes.NewBuffer(bys)
	var data int64
	_ = binary.Read(buffer, binary.BigEndian, &data)
	return data
}
