package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
)

type jsonIndexEntry struct {
	Id          uint32 `json:"id"`
	Layer       string `json:"layer"`
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	RowCount    int64  `json:"row_count"`
	ChunkTime   int64  `json:"chunk_time"`
	MinTime     int64  `json:"min_time"`
	MaxTime     int64  `json:"max_time"`
	Range       string `json:"range"`
	Type        string `json:"type"`
	_marshalled string `json:"-"`
}

type jsonPartIndex struct {
	rootPath string
	database string
	table    string

	idxPath string

	entries   *sync.Map
	promises  []Promise[int32]
	m         sync.Mutex
	updateCtx context.Context
	doUpdate  context.CancelFunc
	workCtx   context.Context
	stop      context.CancelFunc
	lastId    uint32

	dropQueue        []string
	parquetSizeBytes int64
	rowCount         int64
	minTime          int64
	maxTime          int64
	filesInMerge     map[string]bool
}

func newJsonPartIndex(rootPath string, database string, table string, partPath string,
) (*jsonPartIndex, error) {
	res := &jsonPartIndex{
		rootPath: rootPath,
		database: database,
		table:    table,
		idxPath:  path.Join(rootPath, database, table, partPath),
		entries:  &sync.Map{},
	}
	res.updateCtx, res.doUpdate = context.WithCancel(context.Background())
	res.workCtx, res.stop = context.WithCancel(context.Background())
	err := res.populate()
	return res, err
}

func (J *jsonPartIndex) GetMergePlan(layer string, database string, table string, iteration int) (*MergePlan, error) {
	if database != J.database || table != J.table {
		return nil, nil
	}
	suffix := fmt.Sprintf(".%d.parquet", iteration)
	var from []string
	var size int64
	if iteration > len(MergeConfigurations) {
		return nil, fmt.Errorf("no more merge configurations available for iteration %d", iteration)
	}
	conf := MergeConfigurations[iteration-1]
	J.m.Lock()
	defer J.m.Unlock()
	J.entries.Range(func(key, value interface{}) bool {
		entry := value.(*jsonIndexEntry)
		if !strings.HasSuffix(entry.Path, suffix) {
			return true
		}
		if J.filesInMerge[entry.Path] {
			return true
		}
		if size > conf[1] {
			return false
		}
		from = append(from, entry.Path)
		size += entry.SizeBytes
		return true
	})
	for _, file := range from {
		J.filesInMerge[file] = true
	}
	uid, _ := uuid.NewUUID()

	return &MergePlan{
		Database:  J.database,
		Table:     J.table,
		From:      from,
		To:        path.Join(J.idxPath, fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
		Iteration: iteration,
	}, nil
}

func (J *jsonPartIndex) EndMerge(plan *MergePlan) error {
	J.m.Lock()
	defer J.m.Unlock()
	for _, file := range plan.From {
		delete(J.filesInMerge, file)
	}
	return nil
}

func (J *jsonPartIndex) GetQuerier() Querier {
	return J
}

func (J *jsonPartIndex) GetMergePlanner() MergePlanner {
	return J
}

var _ Index = &jsonPartIndex{}

func (J *jsonPartIndex) Query(options QueryOptions) ([]*IndexEntry, error) {
	if options.Database != J.database || options.Table != J.table {
		return nil, nil
	}
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
		if options.Before.Unix() != 0 && _v.MinTime > options.Before.UnixNano() {
			return true
		}
		if options.After.Unix() != 0 && _v.MaxTime < options.After.UnixNano() {
			return true
		}
		res = append(res, J.jEntry2Entry(_v))
		return true
	})
	return res, nil
}

func (J *jsonPartIndex) addToDropQueue(files []string) {
	J.dropQueue = append(J.dropQueue, files...)
}

func (J *jsonPartIndex) RmFromDropQueue(files []string) Promise[int32] {
	J.m.Lock()
	defer J.m.Unlock()

	updated := false
	for i := len(J.dropQueue) - 1; i >= 0; i-- {
		for _, file := range files {
			if J.dropQueue[i] != file {
				continue
			}
			J.dropQueue[i] = J.dropQueue[len(J.dropQueue)-1]
			J.dropQueue = J.dropQueue[:len(J.dropQueue)-1]
			updated = true
			break
		}
	}

	if !updated {
		return Fulfilled[int32](nil, 0)
	}

	p := NewPromise[int32]()
	J.promises = append(J.promises, p)
	J.doUpdate()
	return p
}

func (J *jsonPartIndex) GetDropQueue() []string {
	return J.dropQueue
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
				dropQueueEntry := iterator.ReadString()
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

func (J *jsonPartIndex) Batch(add []*IndexEntry, rm []string) Promise[int32] {
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
			Id:        id,
			Layer:     entry.Layer,
			Path:      entry.Path,
			SizeBytes: entry.SizeBytes,
			RowCount:  entry.RowCount,
			ChunkTime: entry.ChunkTime,
			MinTime:   entry.MinTime,
			MaxTime:   entry.MaxTime,
			Range:     "1h",
			Type:      "compacted",
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

func (J *jsonPartIndex) rm(path []string) bool {
	rm := false
	for _, entry := range path {
		e, ok := J.entries.Load(entry)
		if !ok {
			continue
		}
		_e := e.(*jsonIndexEntry)
		rm = true
		J.rowCount -= _e.RowCount
		J.parquetSizeBytes -= _e.SizeBytes
		J.entries.Delete(entry)
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
		stream.WriteString(d)
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
	return &IndexEntry{
		Path:      _e.Path,
		SizeBytes: _e.SizeBytes,
		RowCount:  _e.RowCount,
		ChunkTime: _e.ChunkTime,
		MinTime:   _e.MinTime,
		MaxTime:   _e.MaxTime,
	}
}

func (J *jsonPartIndex) Get(path string) *IndexEntry {
	e, _ := J.entries.Load(path)
	if e == nil {
		return nil
	}
	_e := e.(*jsonIndexEntry)
	return J.jEntry2Entry(_e)
}
