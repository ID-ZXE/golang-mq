package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

func IntToBytes(n int) []byte {
	buffer := bytes.NewBuffer([]byte{})
	err := binary.Write(buffer, binary.BigEndian, int32(n))
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func BytesToInt(bys []byte) int {
	buffer := bytes.NewBuffer(bys)
	var data int32
	_ = binary.Read(buffer, binary.BigEndian, &data)
	return int(data)
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
