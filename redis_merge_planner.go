package metadata

import (
	"fmt"
	"github.com/google/uuid"
	"path/filepath"
	"strconv"
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
	plan, err := (&redisTaskQueue[redisMergePlan]{
		prefix:      "merge:",
		database:    r.database,
		table:       r.table,
		suffix:      strconv.Itoa(iteration),
		writerId:    writerId,
		layer:       layer,
		getEntrySHA: r.getMergePlanSha,
		redis:       r.c,
	}).processEntry()

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
	err := (&redisTaskQueue[redisMergePlan]{
		prefix:      "merge:",
		database:    r.database,
		table:       r.table,
		suffix:      strconv.Itoa(plan.Iteration),
		writerId:    plan.WriterID,
		layer:       plan.Layer,
		getEntrySHA: r.getMergePlanSha,
		redis:       r.c,
	}).finishProcess(redisMergePlan{ID: plan.ID})
	return Fulfilled(err, int32(0))
}

func (r *RedisIndex) GetMergePlanner() TableMergePlanner {
	return r
}
