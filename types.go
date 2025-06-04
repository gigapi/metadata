package metadata

import (
	"time"
)

// MergeConfiguration is array of arrays of:
// [[timeout_sec, max_size, merge_iteration_id], ...]
// You have to init MergeConfigurations in the very beginning
type MergeConfigurationsConf [][3]int64

var MergeConfigurations MergeConfigurationsConf

type Layer struct {
	URL  string
	Name string
	Type string
	TTL  time.Duration
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

type MergePlan struct {
	ID        string
	Layer     string
	Database  string
	Table     string
	From      []string
	To        string
	Iteration int
}

type MovePlan struct {
	ID        string
	Database  string
	Table     string
	PathFrom  string
	LayerFrom string
	PathTo    string
	LayerTo   string
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
	RmFromDropQueue(layer string, files []string) Promise[int32]
	GetDropQueue(layer string) []string
	GetMergePlanner() TableMergePlanner
	GetQuerier() TableQuerier
	GetMovePlanner() TableMovePlanner
}

type TableMergePlanner interface {
	GetMergePlan(layer string, iteration int) (*MergePlan, error)
	EndMerge(plan *MergePlan) error
}

type TableMovePlanner interface {
	GetMovePlan(layer string) *MovePlan
	EndMove(plan *MovePlan) error
}

type TableQuerier interface {
	Query(options QueryOptions) ([]*IndexEntry, error)
}
