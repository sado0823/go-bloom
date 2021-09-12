package redis

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

// createRedis returns a in process redis.Redis.
func createRedis() (addr string, clean func(), err error) {
	mr, err := miniredis.Run()
	if err != nil {
		return "", nil, err
	}

	return mr.Addr(), func() {
		ch := make(chan struct{})
		go func() {
			mr.Close()
			close(ch)
		}()
		select {
		case <-ch:
		case <-time.After(time.Second):
		}
	}, nil
}

func TestRedisBitSet_New_Set_Test(t *testing.T) {
	addr, clean, err := createRedis()
	assert.Nil(t, err)
	defer clean()

	bitSet := NewRedisProvider(addr, "test_key", 1024)
	isSetBefore, err := bitSet.check([]uint{0})
	if err != nil {
		t.Fatal(err)
	}
	if isSetBefore {
		t.Fatal("Bit should not be set")
	}
	err = bitSet.set([]uint{512})
	if err != nil {
		t.Fatal(err)
	}
	isSetAfter, err := bitSet.check([]uint{512})
	if err != nil {
		t.Fatal(err)
	}
	if !isSetAfter {
		t.Fatal("Bit should be set")
	}

}

func TestRedisBitSet_Add(t *testing.T) {
	addr, clean, err := createRedis()
	assert.Nil(t, err)
	defer clean()

	filter := NewRedisProvider(addr, "test_key", 64)
	assert.Nil(t, filter.Add([]byte("hello")))
	assert.Nil(t, filter.Add([]byte("world")))
	ok, err := filter.Exists([]byte("hello"))
	assert.Nil(t, err)
	assert.True(t, ok)
}
