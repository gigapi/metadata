package metadata

import (
	"fmt"
	"github.com/google/uuid"
	"testing"
	"time"
)

var layers = []Layer{
	{"file://./_testdata", "l1", "fs", 20},
}

func TestSave(t *testing.T) {
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	idx, err := NewRedisIndex(
		"redis://localhost:6379/0",
		"default",
		"test",
		layers)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	var ents []*IndexEntry
	now := time.Now()
	threeDaysAgo := now.Add(-3 * 24 * time.Hour)

	for ts := threeDaysAgo; ts.Before(now); ts = ts.Add(15 * time.Second) {
		ents = append(ents, &IndexEntry{
			Database: "default",
			Table:    "test",
			MinTime:  ts.UnixNano(),
			MaxTime:  ts.Add(15 * time.Second).UnixNano(),
			Path: fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
				ts.UTC().Format("2006-01-02"),
				ts.UTC().Hour(),
				uuid.New().String()),
			SizeBytes: 1000000,
			ChunkTime: time.Now().UnixNano(),
			Layer:     "l1",
		})
	}

	p := idx.Batch(ents, nil)
	_, err = p.Get()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Items saved: %d\n", len(ents))
}

func TestSaveAndDel(t *testing.T) {
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	idx, err := NewRedisIndex(
		"redis://localhost:6379/0",
		"default",
		"test",
		layers)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	var ents []*IndexEntry
	now := time.Now()
	threeDaysAgo := now.Add(-3 * 24 * time.Hour)

	for ts := threeDaysAgo; ts.Before(now); ts = ts.Add(15 * time.Second) {
		pth := fmt.Sprintf("date=%s/hour=%02d/%s.1.parquet",
			ts.UTC().Format("2006-01-02"),
			ts.UTC().Hour(),
			uuid.New().String())
		ents = append(ents, &IndexEntry{
			Database:  "default",
			Table:     "test",
			MinTime:   ts.UnixNano(),
			MaxTime:   ts.Add(15 * time.Second).UnixNano(),
			Path:      pth,
			SizeBytes: 1000000,
			ChunkTime: time.Now().UnixNano(),
			Layer:     "l1",
		})
	}
	start := time.Now()
	p := idx.Batch(ents, nil)
	_, err = p.Get()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d items saved in %v\n", len(ents), time.Since(start))

	time.Sleep(time.Second)

	start = time.Now()
	p = idx.Batch(nil, ents)
	_, err = p.Get()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d items dropped in %v\n", len(ents), time.Since(start))

	time.Sleep(time.Second * 30)

	start = time.Now()
	var drops []DropPlan
	d, err := idx.GetDropPlanner().GetDropQueue("", "l1")
	if err != nil {
		t.Fatalf("Failed to get drop queue: %v", err)
		return
	}
	for d.Path != "" {
		d, err = idx.GetDropPlanner().GetDropQueue("", "l1")
		if err != nil {
			t.Fatalf("Failed to get drop queue: %v", err)
			return
		}
		drops = append(drops, d)
	}
	fmt.Printf("Acquired drop of %d items in %v\n", len(drops), time.Since(start))
	start = time.Now()
	for _, d := range drops[:200] {
		_, err = idx.GetDropPlanner().RmFromDropQueue(d).Get()
		if err != nil {
			t.Fatalf("Failed to remove from drop queue: %v", err)
			return
		}
	}
	fmt.Printf("Processed 200 drop items in %v\n", time.Since(start))
}

func TestRedisIndex2(t *testing.T) {
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	idx, err := NewRedisIndex(
		"redis://localhost:6379/0",
		"default",
		"test",
		layers)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	ies, err := idx.GetQuerier().Query(QueryOptions{
		After: time.Now().Add(-3 * 24 * time.Hour),
	})
	fmt.Println("Total:", len(ies))
}

func TestRedisDBIndex(t *testing.T) {
	MergeConfigurations = []MergeConfigurationsConf{
		{10, 10 * 1024 * 1024, 1},
	}
	idx, err := NewRedisDbIndex("redis://localhost:6379/0")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	dbs, err := idx.Databases()
	fmt.Println("Databases:", dbs)

	tbls, err := idx.Tables("default")
	fmt.Println("Tables:", tbls)

	paths, err := idx.Paths("default", "test")
	fmt.Println("Paths:", paths)
}
