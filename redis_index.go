package metadata

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type redisLayer struct {
	Layer
	LayerTo string `json:"layer_to"`
}

type redisIndexEntry struct {
	IndexEntry
	StrMinTime   string `json:"str_min_time"`
	StrMaxTime   string `json:"str_max_time"`
	StrChunkTime string `json:"str_chunk_time"`
	Cmd          string `json:"cmd"`
}

func indexEntry2Redis(ie *IndexEntry, cmd string) redisIndexEntry {
	r := redisIndexEntry{}
	r.IndexEntry = *ie
	r.StrMinTime = strconv.FormatInt(ie.MinTime, 10)
	r.StrMaxTime = strconv.FormatInt(ie.MaxTime, 10)
	r.StrChunkTime = strconv.FormatInt(ie.ChunkTime, 10)
	r.IndexEntry.MinTime = 0
	r.IndexEntry.MaxTime = 0
	r.IndexEntry.ChunkTime = 0
	r.Cmd = cmd
	return r
}

func (r *redisIndexEntry) ToIndexEntry() *IndexEntry {
	if r.StrMinTime != "" {
		r.IndexEntry.MinTime, _ = strconv.ParseInt(r.StrMinTime, 10, 64)
	}
	if r.StrMaxTime != "" {
		r.IndexEntry.MaxTime, _ = strconv.ParseInt(r.StrMaxTime, 10, 64)
	}
	if r.StrChunkTime != "" {
		r.IndexEntry.ChunkTime, _ = strconv.ParseInt(r.StrChunkTime, 10, 64)
	}
	return &r.IndexEntry
}

type RedisIndex struct {
	url             *url.URL
	c               *redis.Client
	patchSha        string
	getMergePlanSha string
	endMergeSha     string

	database string
	table    string
	layers   []redisLayer
}

func getRedisClient(u *url.URL) (*redis.Client, error) {

	strDb := strings.Trim(u.Path, "/")
	iDb, err := strconv.Atoi(strDb)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis DB number: %s", strDb)
	}

	opts := &redis.Options{
		Addr: fmt.Sprintf("%s", u.Host),
		DB:   iDb,
	}

	if u.User != nil && u.User.Username() != "" {
		opts.Username = u.User.Username()
		if pwd, ok := u.User.Password(); ok {
			opts.Password = pwd
		}
	}
	if u.Scheme == "rediss" {
		opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: true,
			}
			return tls.Dial(network, addr, tlsConfig)
		}
	}

	return redis.NewClient(opts), nil
}

func NewRedisIndex(URL string, database string, table string, layers []Layer) (TableIndex, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	redisLayers := make([]redisLayer, len(layers))
	for i, layer := range layers {
		layerTo := ""
		if i < len(layers)-1 {
			layerTo = layers[i+1].Name
		}
		redisLayers = append(redisLayers, redisLayer{
			Layer:   layer,
			LayerTo: layerTo,
		})
	}

	idx := &RedisIndex{
		url:      u,
		database: database,
		table:    table,
		layers:   redisLayers,
	}

	client, err := getRedisClient(u)
	if err != nil {
		return nil, err
	}
	idx.c = client

	err = idx.initFuncs()
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (r *RedisIndex) GetQuerier() TableQuerier {
	return r
}

func (r *RedisIndex) initFuncs() error {
	var err error
	r.patchSha, err = r.c.ScriptLoad(context.Background(), string(SCRIPT_PATCH_INDEX)).Result()
	if err != nil {
		return err
	}
	r.getMergePlanSha, err = r.c.ScriptLoad(context.Background(), string(GET_MERGE_PLAN_SCRIPT)).Result()
	if err != nil {
		return err
	}
	r.endMergeSha, err = r.c.ScriptLoad(context.Background(), string(END_MERGE_SCRIPT)).Result()
	return err
}

func (r *RedisIndex) Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
	var cmds []any
	for _, entry := range add {
		cmd, err := json.Marshal(indexEntry2Redis(entry, "ADD"))
		if err != nil {
			return Fulfilled[int32](err, 0)
		}
		cmds = append(cmds, string(cmd))
	}
	for _, ie := range rm {
		cmd, err := json.Marshal(indexEntry2Redis(ie, "DELETE"))
		if err != nil {
			return Fulfilled[int32](err, 0)
		}
		cmds = append(cmds, string(cmd))
	}
	res := NewPromise[int32]()

	var err error
	var keys [2][]byte
	keys[0], err = json.Marshal(MergeConfigurations)
	if err != nil {
		res.Done(0, err)
		return res
	}
	lmap := make(map[string]redisLayer)
	for _, layer := range r.layers {
		lmap[layer.Name] = layer
	}
	keys[1], err = json.Marshal(lmap)
	if err != nil {
		res.Done(0, err)
		return res
	}

	go func() {
		_, err := r.c.EvalSha(context.Background(), r.patchSha, []string{
			string(keys[0]),
			string(keys[1]),
		}, cmds...).Result()
		res.Done(0, err)
	}()
	return res
}

func (r *RedisIndex) Get(layer string, path string) *IndexEntry {
	firstFolder := strings.Split(path, "/")[0]
	res, err := r.c.HGet(
		context.Background(),
		fmt.Sprintf("files:%s:%s:%s", r.database, r.table, firstFolder),
		path,
	).Result()
	if err != nil {
		return nil
	}
	e := &redisIndexEntry{}
	err = json.Unmarshal([]byte(res), e)
	if err != nil {
		return nil
	}
	return e.ToIndexEntry()
}

func (r *RedisIndex) Run() {
}

func (r *RedisIndex) Stop() {
}

func redisScan(scanFn func(cursor uint64) (uint64, error)) error {
	var err error
	var cursor uint64 = 0
	for {
		cursor, err = scanFn(cursor)
		if err != nil {
			return err
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (r *RedisIndex) getMainKeys(options QueryOptions) ([]string, error) {
	if options.Folder != "" {
		firstFolder := strings.SplitN(strings.TrimPrefix(options.Folder, "/"), string(os.PathSeparator), 2)[0]
		mainKey := fmt.Sprintf("files:%s:%s:%s", r.database, r.table, firstFolder)
		exist, err := r.c.Exists(context.Background(), mainKey).Result()
		if err != nil {
			return nil, err
		}
		if exist == 0 {
			return nil, nil
		}
		return []string{mainKey}, nil
	}

	if options.After.Unix() > 0 && options.Before.Unix() > 0 {
		var keys []string
		for start := options.After; start.Before(options.Before); start = start.Add(time.Hour * 24) {
			mainKey := fmt.Sprintf("files:%s:%s:date=%s",
				r.database, r.table, start.Format("2006-01-02"))
			exist, err := r.c.Exists(context.Background(), mainKey).Result()
			if err != nil {
				return nil, err
			}
			if exist == 0 {
				continue
			}
			keys = append(keys, mainKey)
		}
		return keys, nil
	}
	pattern := fmt.Sprintf("files:%s:%s:*", r.database, r.table)
	var allKeys []string
	var dayAfter int64 = 0
	if options.After.Unix() > 0 {
		dayAfter = options.After.Truncate(24 * time.Hour).Unix()
	}
	var dayBefore int64 = math.MaxInt64
	if options.Before.Unix() > 0 {
		dayBefore = options.Before.Unix()
	}
	err := redisScan(func(cursor uint64) (uint64, error) {
		keys, cursor, err := r.c.Scan(context.Background(), cursor, pattern, 10000).Result()
		if err != nil {
			return 0, err
		}
		for _, k := range keys {
			keyParts := strings.SplitN(k, ":", 4)
			if len(keyParts) < 3 {
				continue
			}
			date, err := time.Parse("2006-01-02", keyParts[3][5:])
			if err != nil {
				continue
			}
			if dayAfter > date.Unix() {
				continue
			}
			if dayBefore < date.Unix() {
				continue
			}
			allKeys = append(allKeys, k)
		}
		return cursor, nil
	})
	return allKeys, err
}

func (r *RedisIndex) filterKeys(keys []string, day time.Time, options *QueryOptions) []string {
	var hourAfter int64
	if options.After.Unix() > 0 {
		hourAfter = options.After.Truncate(time.Hour).Unix()
	}
	var hourBefore int64 = math.MaxInt64
	if options.Before.Unix() > 0 {
		hourBefore = options.Before.Unix()
	}
	suffix := ""
	if options.Iteration > 0 {
		suffix = fmt.Sprintf(".%d.parquet", options.Iteration)
	}
	var res []string
	for i := 0; i < len(keys); i += 2 {
		k := keys[i]
		if options.Folder != "" && !strings.HasPrefix(k, options.Folder) {
			continue
		}
		if suffix != "" && !strings.HasSuffix(k, suffix) {
			continue
		}

		parts := strings.SplitN(k, "/", 3)
		if len(parts) < 3 {
			continue
		}
		iHour, err := strconv.Atoi(parts[1][5:])
		if err != nil {
			continue
		}
		hour := day.Add(time.Duration(iHour) * time.Hour)
		if hourAfter > hour.Unix() {
			continue
		}
		if hourBefore < hour.Unix() {
			continue
		}
		res = append(res, k, keys[i+1])
	}
	return res
}

func (r *RedisIndex) filterValues(values []string, options *QueryOptions) ([]*IndexEntry, error) {
	var res []*IndexEntry
	for i := 1; i < len(values); i += 2 {
		strV := values[i]
		var ie redisIndexEntry
		err := json.Unmarshal([]byte(strV), &ie)
		if err != nil {
			continue
		}
		ie.ToIndexEntry()
		if options.Before.Unix() > 0 && ie.MinTime > options.Before.UnixNano() {
			continue
		}
		if options.After.Unix() > 0 && ie.MaxTime < options.After.UnixNano() {
			continue
		}
		res = append(res, ie.ToIndexEntry())
	}
	return res, nil
}

func (r *RedisIndex) Query(options QueryOptions) ([]*IndexEntry, error) {
	keys := make(map[string][]string)
	mainKeys, err := r.getMainKeys(options)
	if err != nil {
		return nil, err
	}
	for _, mainKey := range mainKeys {
		parts := strings.SplitN(mainKey, ":", 4)
		day, err := time.Parse("2006-01-02", parts[3][5:])
		if err != nil {
			continue
		}
		_mainKey := mainKey
		err = redisScan(func(cursor uint64) (uint64, error) {
			_keys, cursor, err :=
				r.c.HScan(context.Background(), _mainKey, cursor, "*", 10000).Result()
			if err != nil {
				return 0, err
			}
			_keys = r.filterKeys(_keys, day, &options)
			keys[_mainKey] = append(keys[_mainKey], _keys...)
			return cursor, nil
		})
		if err != nil {
			return nil, err
		}
	}

	var res []*IndexEntry
	for _, files := range keys {
		ies, err := r.filterValues(files, &options)
		if err != nil {
			return nil, err
		}
		res = append(res, ies...)
	}
	return res, nil
}
