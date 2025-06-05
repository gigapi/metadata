package metadata

func (r *RedisIndex) GetMovePlanner() TableMovePlanner {
	return r
}

func (r *RedisIndex) GetMovePlan(writerId string, layer string) (MovePlan, error) {
	return (&redisTaskQueue[MovePlan]{
		prefix:      "move:",
		database:    r.database,
		table:       r.table,
		suffix:      "",
		writerId:    writerId,
		layer:       layer,
		getEntrySHA: r.getMergePlanSha,
		redis:       r.c,
	}).processEntry()
}

func (r *RedisIndex) EndMove(plan MovePlan) Promise[int32] {
	return Fulfilled((&redisTaskQueue[MovePlan]{
		prefix:      "move:",
		database:    r.database,
		table:       r.table,
		suffix:      "",
		writerId:    plan.WriterID,
		layer:       plan.LayerFrom,
		getEntrySHA: r.getMergePlanSha,
		redis:       r.c,
	}).finishProcess(plan), int32(0))
}
