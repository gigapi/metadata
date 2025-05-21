package metadata

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type RedisIndex struct {
	url      *url.URL
	c        *redis.Client
	patchSha string
}

func (r *RedisIndex) GetMergePlan(layer string, database string, table string, iteration int) (*PlanMerge, error) {
}

func (r *RedisIndex) EndMerge(plan *PlanMerge) error {
	//TODO implement me
	panic("implement me")
}

func (r *RedisIndex) GetMergePlanner() MergePlanner {
	return r
}

func (r *RedisIndex) GetQuerier() Querier {
	return r
}

func NewRedisIndex(URL string) (Index, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	strDb := strings.Trim(u.Path, "/")
	iDb, err := strconv.Atoi(strDb)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis DB number: %s", strDb)
	}

	idx := &RedisIndex{url: u}
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

	idx.c = redis.NewClient(opts)

	err = idx.initFuncs()
	if err != nil {
		return nil, err
	}

	return idx, nil
}

type redisIndexEntry struct {
	*IndexEntry
	Cmd string `json:"cmd"`
}

func (r *RedisIndex) initFuncs() error {
	var err error
	r.patchSha, err = r.c.ScriptLoad(context.Background(), SCRIPT_PATCH_INDEX).Result()
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisIndex) Batch(add []*IndexEntry, rm []string) Promise[int32] {
	var cmds []any
	for _, entry := range add {
		cmd, err := json.Marshal(redisIndexEntry{IndexEntry: entry, Cmd: "ADD"})
		if err != nil {
			return Fulfilled[int32](err, 0)
		}
		cmds = append(cmds, string(cmd))
	}
	for _, path := range rm {
		cmd, err := json.Marshal(redisIndexEntry{IndexEntry: &IndexEntry{Path: path}, Cmd: "DELETE"})
		if err != nil {
			return Fulfilled[int32](err, 0)
		}
		cmds = append(cmds, string(cmd))
	}
	res := NewPromise[int32]()

	var keys []string
	for _, c := range MergeConfigurations {
		keys = append(keys, strconv.FormatInt(c[1], 10))
	}

	go func() {
		_, err := r.c.EvalSha(context.Background(), r.patchSha, keys, cmds...).Result()
		res.Done(0, err)
	}()
	return res
}

func (r *RedisIndex) Get(path string) *IndexEntry {
	res, err := r.c.Get(context.Background(), "files:"+path).Result()
	if err != nil {
		return nil
	}
	e := &IndexEntry{}
	err = json.Unmarshal([]byte(res), e)
	if err != nil {
		return nil
	}
	return e
}

func (r *RedisIndex) Run() {
}

func (r *RedisIndex) Stop() {
}

type QEntry struct {
	Path  string `json:"path"`
	TimeS int32  `json:"time"`
}

func (r *RedisIndex) AddToDropQueue(files []string) Promise[int32] {
	_files := make([]any, len(files))
	for i, file := range files {
		_file, err := json.Marshal(QEntry{
			Path:  file,
			TimeS: int32(time.Now().Unix()),
		})
		if err != nil {
			return Fulfilled[int32](err, 0)
		}
		_files[i] = string(_file)
	}
	r.c.LPush(context.Background(), "drop", _files...)
	return Fulfilled[int32](nil, 0)
}

func (r *RedisIndex) RmFromDropQueue(files []string) Promise[int32] {
	res := NewPromise[int32]()
	go func() {
		for _, file := range files {
			_, err := r.c.LRem(context.Background(), "drop", 1, file).Result()
			if err != nil {
				res.Done(0, err)
				return
			}
		}
		res.Done(0, nil)
	}()
	return res
}

func (r *RedisIndex) GetDropQueue() []string {
	res, err := r.c.LRange(context.Background(), "drop", 0, -1).Result()
	if err != nil {
		return nil
	}
	return res
}

func (r *RedisIndex) Query(options QueryOptions) ([]*IndexEntry, error) {
	return nil, fmt.Errorf("not implemented")
}
