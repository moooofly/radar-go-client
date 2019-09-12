package client

import (
	"math/rand"
	"time"
)

// the algorithm of random string generator, check:
// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randStrGenerator() string {
	return string(randBytes(16))
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}

func intToBytes(n uint32) []byte {
	/*
		Python implementation by WuKai:

		def int_to_bytes(n):
			b4 = n & 0xFF
			n >>= 8
			b3 = n & 0xFF
			n >>= 8
			b2 = n & 0xFF
			n >>= 8
			b1 = n & 0xFF
			result = bytes((b1, b2, b3, b4))
			return result
	*/
	var b = make([]byte, 4)

	for i := 3; i >= 0; i-- {
		b[i] = byte(n & 0xFF)
		n >>= 8
	}

	return b
}

func bytesToInt(b []byte) uint32 {
	/*
		Python implementation by WuKai:

		def bytes_to_int(b):
			n = (b[0] << 24) + (b[1] << 16) + (b[2] << 8) + b[3]
			return n
	*/
	return uint32(b[0])<<24 + uint32(b[1])<<16 + uint32(b[2])<<8 + uint32(b[3])
}
