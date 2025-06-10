package metadata

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

type redisMergePlan struct {
	ID    string   `json:"id"`
	Time  int32    `json:"time"`
	Paths []string `json:"paths"`
}

func (r redisMergePlan) Id() string {
	return r.ID
}

func (r *RedisIndex) GetMergePlan(writerId string, layer string, iteration int) (MergePlan, error) {
	mergePattern := fmt.Sprintf("merge:%s:%s:%d:*:%s:%s:*", r.database, r.table, iteration, layer, writerId)
	var keys []string
	err := redisScan(func(cursor uint64) (uint64, error) {
		_keys, cursor, err := r.c.Scan(context.Background(), cursor, mergePattern, 1000).Result()
		if err != nil {
			return 0, err
		}
		keys = append(keys, _keys...)
		return cursor, nil
	})
	if err != nil {
		return MergePlan{}, err
	}
	slices.Sort(keys)
	var plan redisMergePlan
	for _, k := range keys {
		parts := strings.SplitN(k, ":", 6)
		if len(parts) < 6 {
			continue
		}
		dir := parts[4]
		plan, err = (&redisTaskQueue[redisMergePlan]{
			prefix:      "merge",
			database:    r.database,
			table:       r.table,
			suffix:      strconv.Itoa(iteration) + ":" + dir,
			writerId:    writerId,
			layer:       layer,
			getEntrySHA: r.getMergePlanSha,
			redis:       r.c,
		}).processEntry()
		if err != nil {
			return MergePlan{}, err
		}
		if len(plan.Paths) > 0 {
			break
		}
	}

	if len(plan.Paths) == 0 {
		return MergePlan{}, nil
	}

	firstFile := plan.Paths[0]
	firstFileDir := filepath.Dir(firstFile)

	return MergePlan{
		ID:        plan.ID,
		Layer:     layer,
		Database:  r.database,
		Table:     r.table,
		From:      plan.Paths,
		To:        filepath.Join(firstFileDir, fmt.Sprintf("%s.%d.parquet", uuid.New().String(), iteration+1)),
		Iteration: iteration,
		WriterID:  writerId,
	}, err
}

func (r *RedisIndex) EndMerge(plan MergePlan) Promise[int32] {
	fmt.Println("removing merge plan from Redis: ", plan.ID)
	dir := filepath.Dir(plan.To)
	err := (&redisTaskQueue[redisMergePlan]{
		prefix:      "merge",
		database:    r.database,
		table:       r.table,
		suffix:      strconv.Itoa(plan.Iteration) + ":" + dir,
		writerId:    plan.WriterID,
		layer:       plan.Layer,
		getEntrySHA: r.getMergePlanSha,
		redis:       r.c,
	}).finishProcess(redisMergePlan{ID: plan.ID})
	fmt.Println("removing merge plan from Redis ok")
	return Fulfilled(err, int32(0))
}

func (r *RedisIndex) GetMergePlanner() TableMergePlanner {
	return r
}
