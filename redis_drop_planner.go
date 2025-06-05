package metadata

func (r *RedisIndex) GetDropPlanner() TableDropPlanner {
	return r
}

func (r *RedisIndex) RmFromDropQueue(plan DropPlan) Promise[int32] {
	return Fulfilled((&redisTaskQueue[DropPlan]{
		prefix:      "drop",
		database:    r.database,
		table:       r.table,
		suffix:      "",
		writerId:    plan.WriterID,
		layer:       plan.Layer,
		getEntrySHA: r.getMergePlanSha,
		redis:       r.c,
	}).finishProcess(plan), int32(0))
}

func (r *RedisIndex) GetDropQueue(writerId string, layer string) (DropPlan, error) {
	plan, err := (&redisTaskQueue[DropPlan]{
		prefix:      "drop",
		database:    r.database,
		table:       r.table,
		suffix:      "",
		writerId:    writerId,
		layer:       layer,
		getEntrySHA: r.getMergePlanSha,
		redis:       r.c,
	}).processEntry()
	return plan, err
}
