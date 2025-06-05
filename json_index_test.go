package metadata

import (
	"fmt"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestJSONSave(t *testing.T) {
	MergeConfigurations = [][3]int64{
		{10, 10 * 1024 * 1024, 1},
	}
	idx := NewJSONIndex(
		"redis://localhost:6379/0",
		"default",
		"test",
		layers)
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
	_, err := p.Get()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Items saved: %d\n", len(ents))
}

func TestJSONSaveAndRM(t *testing.T) {
	MergeConfigurations = [][3]int64{
		{10, 10 * 1024 * 1024, 1},
	}
	idx := NewJSONIndex(
		"redis://localhost:6379/0",
		"default",
		"test",
		layers)
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
	_, err := p.Get()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Items saved: %d\n", len(ents))

	p = idx.Batch(nil, ents)
	_, err = p.Get()
	if err != nil {
		panic(err)
	}
}
