package redis

import (
	"errors"
	"github.com/go-redis/redis"
	"github.com/spaolacci/murmur3"
	"strconv"
)

const (
	// for detail, see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
	maps      = 14
	setScript = `
for _, offset in ipairs(ARGV) do
	redis.call("setbit", KEYS[1], offset, 1)
end
`
	checkScript = `
for _, offset in ipairs(ARGV) do
	if tonumber(redis.call("getbit", KEYS[1], offset)) == 0 then
		return false
	end
end
return true
`
)

var ErrTooLargeOffset = errors.New("too large offset")

type Provider struct {
	store *redis.Client
	key   string
	bits  uint
}

func NewRedisProvider(addr string, key string, bits uint) *Provider {
	return &Provider{store: newRedis(addr), key: key, bits: bits}
}

// Add implement Provider interface
func (r *Provider) Add(data []byte) error {
	location := r.getBitLocation(data)
	return r.set(location)
}

// Exists implement Provider interface
func (r *Provider) Exists(data []byte) (bool, error) {
	location := r.getBitLocation(data)
	return r.check(location)
}

func newRedis(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}

// getBitLocation return data hash to bit location
func (r *Provider) getBitLocation(data []byte) []uint {
	l := make([]uint, maps)
	for i := 0; i < maps; i++ {
		hashV := r.hash(append(data, byte(i)))
		l[i] = uint(hashV % uint64(maps))
	}
	return l
}

// set those offsets into bloom filter
func (r *Provider) set(offsets []uint) error {
	args, err := r.buildOffsetArgs(offsets)
	if err != nil {
		return err
	}

	eval := r.store.Eval(setScript, []string{r.key}, args)
	if eval.Err() == redis.Nil {
		return nil
	}

	return eval.Err()
}

// check if those offsets are in bloom filter
func (r *Provider) check(offsets []uint) (bool, error) {
	args, err := r.buildOffsetArgs(offsets)
	if err != nil {
		return false, err
	}

	eval := r.store.Eval(checkScript, []string{r.key}, args)
	if eval.Err() == redis.Nil {
		return false, nil
	} else if eval.Err() != nil {
		return false, eval.Err()
	}

	i, err := eval.Int64()
	if err != nil {
		return false, nil
	}

	return i == 1, nil
}

// buildOffsetArgs set []uint offset to []string that can use in redis
// and check if offset is larger than r.bits
func (r *Provider) buildOffsetArgs(offsets []uint) ([]string, error) {
	var args []string

	for _, offset := range offsets {
		if offset >= r.bits {
			return nil, ErrTooLargeOffset
		}

		args = append(args, strconv.FormatUint(uint64(offset), 10))

	}

	return args, nil
}

// hash returns the hash value of data.
func (r *Provider) hash(data []byte) uint64 {
	return murmur3.Sum64(data)
}
