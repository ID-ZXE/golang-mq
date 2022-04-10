package test

import (
	"broker/utils"
	"testing"
)

func TestInt2Byte(t *testing.T) {
	bytes := utils.Int32ToBytes(1000)
	for _, v := range bytes {
		print(v, " ")
	}
	println()
}

func int2ByteLH(n int32) []byte {
	var bytes = make([]byte, 4)
	bytes[0] = (byte)(n & 0xff)
	bytes[1] = (byte)(n >> 8 & 0xff)
	bytes[2] = (byte)(n >> 16 & 0xff)
	bytes[3] = (byte)(n >> 24 & 0xff)
	return bytes
}

func int2ByteHH(n int32) []byte {
	var bytes = make([]byte, 4)
	bytes[3] = (byte)(n & 0xff)
	bytes[2] = (byte)(n >> 8 & 0xff)
	bytes[1] = (byte)(n >> 16 & 0xff)
	bytes[0] = (byte)(n >> 24 & 0xff)
	return bytes
}
