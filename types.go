package metadata

import (
	"time"
)

// MergeConfiguration is array of arrays of:
// [[timeout_sec, max_size, merge_iteration_id], ...]
// You have to init MergeConfigurations in the very beginning
type MergeConfigurationsConf [][3]int64

var MergeConfigurations MergeConfigurationsConf

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
}

type QueryOptions struct {
	Database  string
	Table     string
	Folder    string
	After     time.Time
	Before    time.Time
	Iteration int
}

type MergePlan struct {
	ID        string
	Layer     string
	Database  string
	Table     string
	From      []string
	To        string
	Iteration int
}

type DBIndex interface {
	Databases() ([]string, error)
	Tables(database string) ([]string, error)
	Paths(database string, table string) ([]string, error)
}

type TableIndex interface {
	Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32]
	Get(path string) *IndexEntry
	Run()
	Stop()
	RmFromDropQueue(files []string) Promise[int32]
	GetDropQueue() []string
	GetMergePlanner() MergePlanner
	GetQuerier() Querier
}

type MergePlanner interface {
	GetMergePlan(layer string, database string, table string, iteration int) (*MergePlan, error)
	EndMerge(plan *MergePlan) error
}

type Querier interface {
	Query(options QueryOptions) ([]*IndexEntry, error)
}
