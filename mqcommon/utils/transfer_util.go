package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
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

func ToJsonString(obj interface{}) string {
	result, err := json.Marshal(obj)
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}
	return string(result)
}

func ToJsonByteArr(obj interface{}) []byte {
	result, err := json.Marshal(obj)
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}
	return result
}

func JsonBytesToBean(byteArr []byte, v interface{}) {
	_ = json.Unmarshal(byteArr, v)
}

func JsonStringToBean(jsonStr string, v interface{}) {
	_ = json.Unmarshal([]byte(jsonStr), v)
}
