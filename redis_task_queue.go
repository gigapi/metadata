package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type redisTaskQueue[T Identified] struct {
	prefix   string
	database string
	table    string
	suffix   string
	writerId string
	layer    string

	getEntrySHA string
	redis       *redis.Client
}

func (q *redisTaskQueue[T]) processEntry() (T, error) {
	var res T
	eStr, err := q.redis.EvalSha(context.Background(), q.getEntrySHA, []string{
		q.prefix,
		q.database,
		q.table,
		q.suffix,
		q.layer,
		q.writerId,
	}, nil).Result()
	if err != nil {
		return res, err
	}
	if eStr == "" {
		return res, nil
	}

	err = json.Unmarshal([]byte(eStr.(string)), &res)

	return res, err
}

func (q *redisTaskQueue[T]) finishProcess(entry T) error {
	suffix := q.suffix
	if suffix != "" {
		suffix += ":"
	}
	key := fmt.Sprintf("%s:%s:%s:%s%s:%s:processing",
		q.prefix, q.database, q.table, q.suffix, q.layer, q.writerId)
	strQ, err := q.redis.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		return err
	}
	for _, e := range strQ {
		var _e T
		err = json.Unmarshal([]byte(e), &_e)
		if err != nil {
			continue
		}
		if _e.Id() == entry.Id() {
			_, err := q.redis.LRem(context.Background(), key, 1, e).Result()
			return err
		}
	}
	return nil
}

func (q *redisTaskQueue[T]) AddEntry(entry T) Promise[int32] {
	//TODO: not implemented
	return nil
}
