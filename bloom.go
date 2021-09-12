package main

import (
	"github.com/sado0823/go-bloom/internal/redis"
)

type (
	// Filter is a bloom filter
	Filter struct {

		// todo counter
		total int64
		hit   int64
		miss  int64

		Provider
	}

	Provider interface {
		Add(data []byte) error
		Exists(data []byte) (bool, error)
	}
)

// NewRedis return a bloom filter base on redis
// addr - redis host like `127.0.0.1:6379`
// key - bloom filter key
// bits - bloom filter size
func NewRedis(addr string, key string, bits uint) *Filter {
	provider := redis.NewRedisProvider(addr, key, bits)
	return &Filter{
		Provider: provider,
	}
}

func NewWithProvider(provider Provider) *Filter {
	return &Filter{Provider: provider}
}
