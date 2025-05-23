package metadata

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net/url"
	"strings"
)

type redisDbIndex struct {
	url *url.URL
	c   *redis.Client
}

func NewRedisDbIndex(URL string) (DBIndex, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	idx := &redisDbIndex{url: u}

	c, err := getRedisClient(u)
	if err != nil {
		return nil, err
	}
	idx.c = c
	return idx, nil
}

func (r *redisDbIndex) Databases() ([]string, error) {
	databases := make(map[string]bool)
	err := redisScan(func(cursor uint64) (uint64, error) {
		keys, cursor, err := r.c.Scan(context.Background(), cursor, "folders:*", 1000).Result()
		if err != nil {
			return 0, err
		}
		for _, key := range keys {
			db := strings.Split(key, ":")[1]
			databases[db] = true
		}
		return cursor, nil
	})
	var dbs []string
	for db := range databases {
		dbs = append(dbs, db)
	}
	return dbs, err
}

func (r *redisDbIndex) Tables(database string) ([]string, error) {
	tables := make(map[string]bool)
	match := fmt.Sprintf("folders:%s:*", database)
	err := redisScan(func(cursor uint64) (uint64, error) {
		keys, cursor, err := r.c.Scan(context.Background(), cursor, match, 1000).Result()
		if err != nil {
			return 0, err
		}
		for _, key := range keys {
			db := strings.Split(key, ":")[2]
			tables[db] = true
		}
		return cursor, nil
	})
	var res []string
	for db := range tables {
		res = append(res, db)
	}
	return res, err
}

func (r *redisDbIndex) Paths(database string, table string) ([]string, error) {
	var paths []string
	key := fmt.Sprintf("folders:%s:%s", database, table)
	err := redisScan(func(cursor uint64) (uint64, error) {
		keys, cursor, err := r.c.HScan(context.Background(), key, cursor, "*", 1000).Result()
		if err != nil {
			return 0, err
		}
		for i := 0; i < len(keys); i += 2 {
			paths = append(paths, keys[i])
		}
		return cursor, nil
	})
	return paths, err
}
