package metadata

import (
	"context"
	"github.com/redis/go-redis/v9"
	"net/url"
)

type redisKVStoreIndex struct {
	URL *url.URL

	c *redis.Client
}

func NewRedisKVStore(URL string) (KVStoreIndex, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	idx := &redisKVStoreIndex{URL: u}

	c, err := getRedisClient(u)
	if err != nil {
		return nil, err
	}
	idx.c = c
	return idx, nil
}

func (r *redisKVStoreIndex) Get(key string) ([]byte, error) {
	val, err := r.c.Get(context.Background(), key).Result()
	if err != nil {
		return nil, nil
	}
	return []byte(val), nil
}

func (r redisKVStoreIndex) Put(key string, value []byte) error {
	return r.c.Set(context.Background(), key, value, 0).Err()
}

func (r redisKVStoreIndex) Delete(key string) error {
	return r.c.Del(context.Background(), key).Err()
}

func (r redisKVStoreIndex) Destroy() {
	r.c.Close()
}
