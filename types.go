package metadata

import (
	"time"
)

// MergeConfiguration is array of arrays of:
// [[timeout_sec, max_size, merge_iteration_id], ...]
// You have to init MergeConfigurations in the very beginning
type MergeConfigurationsConf [3]int64

func (m MergeConfigurationsConf) TimeoutSec() int64 {
	return m[0]
}

func (m MergeConfigurationsConf) MaxSize() int64 {
	return m[1]
}

func (m MergeConfigurationsConf) MergeIterationId() int64 {
	return m[2]
}

var MergeConfigurations []MergeConfigurationsConf

type Layer struct {
	URL    string `json:"url"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	TTLSec int32  `json:"ttl_sec"`
}

type IndexEntry struct {
	Layer     string         `json:"layer"`
	Database  string         `json:"database"`
	Table     string         `json:"table"`
	Path      string         `json:"path"`
	SizeBytes int64          `json:"size_bytes"`
	RowCount  int64          `json:"row_count"`
	ChunkTime int64          `json:"chunk_time"`
	Min       map[string]any `json:"min"`
	Max       map[string]any `json:"max"`
	MinTime   int64          `json:"min_time"`
	MaxTime   int64          `json:"max_time"`
	WriterID  string         `json:"writer_id"`
}

type QueryOptions struct {
	Folder    string
	After     time.Time
	Before    time.Time
	Iteration int
}

type Identified interface {
	Id() string
}

type MergePlan struct {
	ID        string
	WriterID  string
	Layer     string
	Database  string
	Table     string
	From      []string
	To        string
	Iteration int
}

func (m MergePlan) Id() string {
	return m.ID
}

type MovePlan struct {
	ID        string `json:"id"`
	WriterID  string `json:"writer_id"`
	Database  string `json:"database"`
	Table     string `json:"table"`
	PathFrom  string `json:"path_from"`
	LayerFrom string `json:"layer_from"`
	PathTo    string `json:"path_to"`
	LayerTo   string `json:"layer_to"`
}

func (m MovePlan) Id() string {
	return m.ID
}

type DropPlan struct {
	ID       string
	WriterID string
	Layer    string
	Database string
	Table    string
	Path     string
	TimeS    int32
}

func (d DropPlan) Id() string {
	return d.ID
}

type KVStoreIndex interface {
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Delete(key string) error
	Destroy()
}

type DBIndex interface {
	Databases() ([]string, error)
	Tables(database string) ([]string, error)
	Paths(database string, table string) ([]string, error)
}

type TableIndex interface {
	Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32]
	Get(layer string, path string) *IndexEntry
	Run()
	Stop()
	GetMergePlanner() TableMergePlanner
	GetQuerier() TableQuerier
	GetMovePlanner() TableMovePlanner
	GetDropPlanner() TableDropPlanner
}

type TableDropPlanner interface {
	GetDropQueue(writerId string, layer string) (DropPlan, error)
	RmFromDropQueue(plan DropPlan) Promise[int32]
}

type TableMergePlanner interface {
	GetMergePlan(writerId string, layer string, iteration int) (MergePlan, error)
	EndMerge(plan MergePlan) Promise[int32]
}

type TableMovePlanner interface {
	GetMovePlan(writerId string, layer string) (MovePlan, error)
	EndMove(plan MovePlan) Promise[int32]
}

type TableQuerier interface {
	Query(options QueryOptions) ([]*IndexEntry, error)
}
