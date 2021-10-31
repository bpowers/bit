package main

import (
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
)

const (
	nPairs    = 1000000
	prefix    = "pref_"
	suffixLen = 16
	hmacKey   = "d259c7f656caf7f1"
)

func newRand() *rand.Rand {
	var seedBytes [8]byte
	crand.Read(seedBytes[:])
	seed := int64(binary.LittleEndian.Uint64(seedBytes[:]))
	return rand.New(rand.NewSource(seed))
}

func main() {
	rng := newRand()
	h := hmac.New(sha256.New, []byte(hmacKey))

	for i := 0; i < nPairs; i++ {
		var buf [suffixLen / 2]byte
		if _, err := rng.Read(buf[:]); err != nil {
			panic(err)
		}
		value := fmt.Sprintf("%s%x", prefix, buf)
		h.Reset()
		h.Write([]byte(value))
		key := hex.EncodeToString(h.Sum(nil))

		fmt.Printf("%s:%s\n", key, value)
	}
}
