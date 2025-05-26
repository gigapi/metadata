package metadata

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type JSONIndex struct {
	root     string
	database string
	table    string
	lock     sync.Mutex
	parts    map[string]*jsonPartIndex
}

func NewJSONIndex(root string, database string, table string) TableIndex {
	return &JSONIndex{
		root:     root,
		database: database,
		table:    table,
	}
}

func (J *JSONIndex) GetMergePlan(layer string, iteration int) (*MergePlan, error) {
	J.lock.Lock()
	defer J.lock.Unlock()
	for _, part := range J.parts {
		plan, err := part.GetMergePlan(layer, iteration)
		if err != nil {
			return nil, err
		}
		if plan == nil || len(plan.From) == 0 {
			continue
		}
		return plan, nil
	}
	return nil, nil
}

func (J *JSONIndex) EndMerge(plan *MergePlan) error {
	if len(plan.From) == 0 {
		return nil
	}
	J.lock.Lock()
	defer J.lock.Unlock()
	dir := path.Dir(plan.From[0])
	part := J.parts[dir]
	if part != nil {
		return part.EndMerge(plan)
	}
	return nil
}

func (J *JSONIndex) GetMergePlanner() TableMergePlanner {
	return J
}

func (J *JSONIndex) GetQuerier() TableQuerier {
	return J
}

func (J *JSONIndex) Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
	addByPath := make(map[string][]*IndexEntry)
	rmByPath := make(map[string][]*IndexEntry)
	paths := make(map[string]bool)
	for _, entry := range add {
		_path := path.Dir(entry.Path)
		addByPath[_path] = append(addByPath[_path], entry)
		paths[_path] = true
	}
	for _, entry := range rm {
		_path := path.Dir(entry.Path)
		rmByPath[_path] = append(rmByPath[_path], entry)
		paths[_path] = true
	}

	J.lock.Lock()
	defer J.lock.Unlock()

	var promises []Promise[int32]
	for partPath, _ := range paths {
		idx, err := J.populate(partPath)
		if err != nil {
			//TODO: we should do something with the error
			continue
		}
		promises = append(promises, idx.Batch(addByPath[partPath], rmByPath[partPath]))
	}
	return NewWaitForAll[int32](promises)
}

func (J *JSONIndex) populate(dir string) (*jsonPartIndex, error) {
	idx := J.parts[dir]
	if idx != nil {
		return idx, nil
	}
	idx, err := newJsonPartIndex(J.root, J.database, J.table, dir)
	if err != nil {
		return nil, err
	}
	idx.Run()
	J.parts[dir] = idx
	return idx, nil
}

func (J *JSONIndex) Get(_path string) *IndexEntry {
	dir := path.Dir(_path)
	J.lock.Lock()
	defer J.lock.Unlock()
	idx, err := J.populate(dir)
	if err != nil {
		return nil
	}
	return idx.Get(_path)
}

func (J *JSONIndex) Run() {
}

func (J *JSONIndex) Stop() {
	for _, idx := range J.parts {
		idx.Stop()
	}
}

func (J *JSONIndex) RmFromDropQueue(files []string) Promise[int32] {
	filesByPath := make(map[string][]string)
	for _, file := range files {
		_path := path.Dir(file)
		filesByPath[_path] = append(filesByPath[_path], file)
	}
	J.lock.Lock()
	defer J.lock.Unlock()

	var promises []Promise[int32]
	for partPath, files := range filesByPath {
		idx, err := J.populate(partPath)
		if err != nil {
			//TODO: we should do something with the error
			continue
		}
		promises = append(promises, idx.RmFromDropQueue(files))
	}
	return NewWaitForAll[int32](promises)
}

func (J *JSONIndex) GetDropQueue() []string {
	var queue []string
	for _, idx := range J.parts {
		queue = append(queue, idx.GetDropQueue()...)
	}
	return queue
}

func (J *JSONIndex) findHours(options QueryOptions) ([]time.Time, error) {
	var hours []time.Time
	filepath.Walk(path.Join(J.root, J.database, J.table), func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}
		if strings.HasPrefix(info.Name(), "hour=") {
			if options.Folder != "" && path != options.Folder {
				return filepath.SkipDir
			}
			hour, err := strconv.Atoi(info.Name()[5:])
			if err != nil {
				return err
			}
			dirs := strings.Split(path, fmt.Sprintf("%v", filepath.Separator))
			dateDir := dirs[len(dirs)-2]
			date, err := time.Parse("2006-01-02", dateDir[5:])
			if err != nil {
				return err
			}
			date = date.Add(time.Hour * time.Duration(hour))
			hours = append(hours, date)
			return filepath.SkipDir
		}
		return nil
	})
	var _hours []time.Time
	if options.Before.Unix() != 0 {
		for i := len(hours) - 1; i >= 0; i-- {
			if hours[i].Unix() < options.Before.Unix() {
				_hours = append(_hours, hours[i])
			}
		}
		hours = _hours
	}
	if options.After.Unix() != 0 {
		_after := time.Unix(options.After.Unix(), 0).Round(time.Hour)
		for _, hour := range hours {
			if hour.Unix() >= _after.Unix() {
				_hours = append(_hours, hour)
			}
		}
		hours = _hours
	}

	return hours, nil
}

func (J *JSONIndex) Query(options QueryOptions) ([]*IndexEntry, error) {
	hours, err := J.findHours(options)
	if err != nil {
		return nil, err
	}
	var entries []*IndexEntry
	for _, hour := range hours {
		idx, err := J.populate(path.Join(
			fmt.Sprintf("date=%s", hour.Format("2006-01-02")),
			fmt.Sprintf("hour=%02d", hour.Hour())))
		if err != nil {
			return nil, err
		}
		_entries, err := idx.Query(options)
		if err != nil {
			return nil, err
		}
		entries = append(entries, _entries...)
	}
	return entries, nil
}
