package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type jsonIndexEntry struct {
	IndexEntry
	Id          uint32 `json:"id"`
	Range       string `json:"range"`
	Type        string `json:"type"`
	_marshalled string `json:"-"`
}

type jsonPartIdxOpts struct {
	rootPath string
	database string
	table    string
	partPath string
	layers   []jsonLayer
	layer    string
}

type jsonPartIndex struct {
	rootPath string
	database string
	table    string
	layer    string
	layers   []jsonLayer

	idxPath string

	entries   *sync.Map
	promises  []Promise[int32]
	m         sync.Mutex
	updateCtx context.Context
	doUpdate  context.CancelFunc
	workCtx   context.Context
	stop      context.CancelFunc
	lastId    uint32

	dropQueue        []DropPlan
	parquetSizeBytes int64
	rowCount         int64
	minTime          int64
	maxTime          int64
	filesInMerge     map[string]bool
	filesInMove      map[string]bool
}

var _ TableIndex = &jsonPartIndex{}

func newJsonPartIndex(opts jsonPartIdxOpts) (*jsonPartIndex, error) {
	res := &jsonPartIndex{
		rootPath:     opts.rootPath,
		database:     opts.database,
		table:        opts.table,
		idxPath:      path.Join(opts.rootPath, opts.database, opts.table, "data", opts.partPath),
		entries:      &sync.Map{},
		filesInMerge: make(map[string]bool),
		layers:       opts.layers,
	}
	_, err := os.Stat(res.idxPath)
	if os.IsNotExist(err) {
		os.MkdirAll(res.idxPath, 0o755)
	}
	res.updateCtx, res.doUpdate = context.WithCancel(context.Background())
	res.workCtx, res.stop = context.WithCancel(context.Background())
	err = res.populate()
	return res, err
}

func (J *jsonPartIndex) getLayer(name string) int {
	for i, layer := range J.layers {
		if layer.Name == name {
			return i
		}
	}
	return -1
}

func (J *jsonPartIndex) GetQuerier() TableQuerier {
	return J
}

func (J *jsonPartIndex) Query(options QueryOptions) ([]*IndexEntry, error) {
	var res []*IndexEntry
	var suffix string
	if options.Iteration != 0 {
		suffix = fmt.Sprintf(".%d.parquet", options.Iteration)
	}
	J.entries.Range(func(key, value interface{}) bool {
		_v := value.(*jsonIndexEntry)
		if suffix != "" && !strings.HasSuffix(_v.Path, suffix) {
			return true
		}
		if options.Before.Unix() > 0 && _v.MinTime > options.Before.UnixNano() {
			return true
		}
		if options.After.Unix() > 0 && _v.MaxTime < options.After.UnixNano() {
			return true
		}
		res = append(res, J.jEntry2Entry(_v))
		return true
	})
	return res, nil
}

func (J *jsonPartIndex) addToDropQueue(files []*IndexEntry) {
	for _, f := range files {
		J.dropQueue = append(J.dropQueue, DropPlan{
			WriterID: f.WriterID,
			Layer:    f.Layer,
			Database: f.Database,
			Table:    f.Table,
			Path:     f.Path,
			TimeS:    int32(time.Now().Add(time.Second * 30).Unix()),
		})
	}

}

func (J *jsonPartIndex) populate() error {
	partPath := J.idxPath
	if _, err := os.Stat(path.Join(partPath, "metadata.json")); os.IsNotExist(err) {
		return nil
	}

	f, err := os.Open(path.Join(partPath, "metadata.json"))
	if err != nil {
		return err
	}
	defer f.Close()

	iter := jsoniter.Parse(jsoniter.ConfigDefault, f, 4096)
	iter.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
		switch s {
		case "drop_queue":
			for iterator.ReadArray() {
				var dropQueueEntry DropPlan
				iterator.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
					switch s {
					case "writer_id":
						dropQueueEntry.WriterID = iterator.ReadString()
					case "layer":
						dropQueueEntry.Layer = iterator.ReadString()
					case "database":
						dropQueueEntry.Database = iterator.ReadString()
					case "table":
						dropQueueEntry.Table = iterator.ReadString()
					case "path":
						dropQueueEntry.Path = iterator.ReadString()
					case "time_s":
						dropQueueEntry.TimeS = iterator.ReadInt32()
					default:
						iterator.Skip()
					}
					return true
				})
				J.dropQueue = append(J.dropQueue, dropQueueEntry)
			}
		case "type":
			iterator.Skip()
		case "parquet_size_bytes":
			J.parquetSizeBytes = iterator.ReadInt64()
		case "row_count":
			J.rowCount = iterator.ReadInt64()
		case "min_time":
			J.minTime = iterator.ReadInt64()
		case "max_time":
			J.maxTime = iterator.ReadInt64()
		case "wal_sequence":
			iterator.Skip()
		case "files":
			err = J.populateFiles(iterator)
			if err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	if iter.Error != nil {
		return iter.Error
	}
	return nil
}

func (J *jsonPartIndex) populateFiles(iter *jsoniter.Iterator) error {
	for iter.ReadArray() {
		e := &jsonIndexEntry{}
		iter.ReadVal(e)
		_marshalled, err := json.Marshal(e)
		if err != nil {
			return err
		}
		e._marshalled = string(_marshalled)
		if e.Id > J.lastId {
			J.lastId = e.Id
		}
		J.entries.Store(e.Path, e)
	}
	return nil
}

func (J *jsonPartIndex) Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
	_add, err := J.entry2JEntry(add)
	if err != nil {
		return Fulfilled[int32](err, 0)
	}
	J.m.Lock()
	defer J.m.Unlock()
	J.add(_add)
	removed := J.rm(rm)
	if len(_add) == 0 && !removed {
		return Fulfilled(nil, int32(0))
	}
	J.addToDropQueue(rm)
	p := NewPromise[int32]()
	J.promises = append(J.promises, p)
	J.doUpdate()
	return p
}

func (J *jsonPartIndex) entry2JEntry(entries []*IndexEntry) ([]*jsonIndexEntry, error) {
	res := make([]*jsonIndexEntry, len(entries))
	for i, entry := range entries {
		id := atomic.AddUint32(&J.lastId, 1)
		_entry := &jsonIndexEntry{
			Id:         id,
			IndexEntry: *entry,
			Range:      "1h",
			Type:       "compacted",
		}
		_marshalled, err := json.Marshal(_entry)
		if err != nil {
			return nil, err
		}
		_entry._marshalled = string(_marshalled)
		res[i] = _entry
	}
	return res, nil
}

func (J *jsonPartIndex) add(entries []*jsonIndexEntry) {
	for _, entry := range entries {
		J.rowCount += entry.RowCount
		J.parquetSizeBytes += entry.SizeBytes
		J.entries.Store(entry.Path, entry)
		if entry.Id == 1 {
			J.minTime = entry.MinTime
			J.maxTime = entry.MaxTime
			continue
		}
		if entry.MinTime != 0 {
			J.minTime = min(J.minTime, entry.MinTime)
		}
		if entry.MinTime != 0 {
			J.maxTime = max(J.maxTime, entry.MaxTime)
		}
	}
}

func (J *jsonPartIndex) recalcMin() {
	if J.entries == nil {
		J.minTime = 0
		return
	}
	var i int
	J.entries.Range(func(key, value interface{}) bool {
		entry := value.(*jsonIndexEntry)
		if i == 0 {
			J.minTime = entry.MinTime
			i++
		}
		J.minTime = min(J.minTime, entry.MinTime)
		return true
	})
}

func (J *jsonPartIndex) recalcMax() {
	if J.entries == nil {
		J.maxTime = 0
		return
	}
	var i int
	J.entries.Range(func(key, value interface{}) bool {
		entry := value.(*jsonIndexEntry)
		if i == 0 {
			J.maxTime = entry.MaxTime
			i++
		}
		J.maxTime = max(J.maxTime, entry.MaxTime)
		return true
	})
}

func (J *jsonPartIndex) rm(path []*IndexEntry) bool {
	rm := false
	for _, entry := range path {
		e, ok := J.entries.Load(entry.Path)
		if !ok {
			continue
		}
		_e := e.(*jsonIndexEntry)
		rm = true
		J.rowCount -= _e.RowCount
		J.parquetSizeBytes -= _e.SizeBytes
		J.entries.Delete(entry.Path)
		if _e.MinTime == J.minTime {
			J.recalcMin()
		}
		if _e.MaxTime == J.maxTime {
			J.recalcMax()
		}
	}
	return rm
}

func (J *jsonPartIndex) flush() {
	J.m.Lock()
	J.updateCtx, J.doUpdate = context.WithCancel(context.Background())
	var entries []string
	dropQueue := J.dropQueue
	parquetSizeBytes := J.parquetSizeBytes
	promises := J.promises
	J.promises = nil
	rowCount := J.rowCount
	minTime := J.minTime
	maxTime := J.maxTime
	J.entries.Range(func(key, value any) bool {
		entries = append(entries, value.(*jsonIndexEntry)._marshalled)
		return true
	})
	J.m.Unlock()

	onErr := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}

	f, err := os.Create(path.Join(J.idxPath, "metadata.json.bak"))
	if err != nil {
		onErr(err)
		return
	}
	defer f.Close()

	stream := jsoniter.NewStream(jsoniter.ConfigDefault, f, 4096)

	// Start encoding the JSON structure
	stream.WriteObjectStart()

	stream.WriteObjectField("type")
	stream.WriteString(J.table)

	stream.WriteMore()
	stream.WriteObjectField("parquet_size_bytes")
	stream.WriteInt64(parquetSizeBytes)

	stream.WriteMore()
	stream.WriteObjectField("row_count")
	stream.WriteInt64(rowCount)

	stream.WriteMore()
	stream.WriteObjectField("min_time")
	stream.WriteInt64(minTime)

	stream.WriteMore()
	stream.WriteObjectField("max_time")
	stream.WriteInt64(maxTime)

	stream.WriteMore()
	stream.WriteObjectField("wal_sequence")
	stream.WriteInt64(0)

	stream.WriteMore()
	stream.WriteObjectField("drop_queue")
	stream.WriteArrayStart()
	for i, d := range dropQueue {
		if i > 0 {
			stream.WriteMore()
		}
		strD, err := json.Marshal(d)
		if err != nil {
			onErr(err)
			return
		}
		stream.WriteString(string(strD))
	}
	stream.WriteArrayEnd()

	stream.WriteMore()
	stream.WriteObjectField("files")
	stream.WriteArrayStart()

	// Write the entries
	for i, entry := range entries {
		if i > 0 {
			stream.WriteMore()
		}
		stream.WriteRaw(entry)
	}

	// Close the array and object
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()

	if stream.Error != nil {
		onErr(stream.Error)
		return
	}

	err = stream.Flush()
	if err != nil {
		onErr(err)
		return
	}

	// Rename the backup file to the actual metadata file
	err = os.Rename(path.Join(J.idxPath, "metadata.json.bak"), path.Join(J.idxPath, "metadata.json"))
	if err != nil {
		onErr(err)
		return
	}

	onErr(nil)
}

func (J *jsonPartIndex) Run() {
	go func() {
		for {
			select {
			case <-J.updateCtx.Done():

				J.flush()
			case <-J.workCtx.Done():
				return
			}
		}
	}()
}

func (J *jsonPartIndex) Stop() {
	J.stop()
}

func (J *jsonPartIndex) jEntry2Entry(_e *jsonIndexEntry) *IndexEntry {
	return &_e.IndexEntry
}

func (J *jsonPartIndex) Get(layer string, path string) *IndexEntry {
	e, _ := J.entries.Load(path)
	if e == nil {
		return nil
	}
	_e := e.(*jsonIndexEntry)
	return J.jEntry2Entry(_e)
}
