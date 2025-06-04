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

type jsonLayer struct {
	Layer
	Path string
}

func layer2JsonLayer(layer Layer) jsonLayer {
	path := ""
	if strings.HasPrefix(layer.URL, "file://") {
		path = strings.TrimPrefix(layer.URL, "file://")
	}
	return jsonLayer{
		Layer: layer,
		Path:  path,
	}
}

type JSONIndex struct {
	root     string
	database string
	table    string
	lock     sync.Mutex
	parts    map[string]map[string]*jsonPartIndex
	layers   []jsonLayer
}

func NewJSONIndex(root string, database string, table string, layers []Layer) TableIndex {
	var jLayers []jsonLayer
	for _, layer := range layers {
		jLayers = append(jLayers, layer2JsonLayer(layer))
	}
	return &JSONIndex{
		root:     root,
		database: database,
		table:    table,
		parts:    map[string]map[string]*jsonPartIndex{},
		layers:   jLayers,
	}
}

func (J *JSONIndex) GetMergePlan(layer string, iteration int) (*MergePlan, error) {
	J.lock.Lock()
	defer J.lock.Unlock()
	parts, ok := J.parts[layer]
	if !ok {
		return nil, nil
	}
	for _, part := range parts {
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
	parts := J.parts[plan.Layer]
	dir := path.Dir(plan.From[0])
	part := parts[dir]
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
	J.lock.Lock()
	defer J.lock.Unlock()
	addByLayer := make(map[string][]*IndexEntry)
	rmByLayer := make(map[string][]*IndexEntry)
	layers := make(map[string]bool)
	for _, entry := range add {
		addByLayer[entry.Layer] = append(addByLayer[entry.Layer], entry)
		layers[entry.Layer] = true
	}
	for _, entry := range rm {
		rmByLayer[entry.Layer] = append(rmByLayer[entry.Layer], entry)
		layers[entry.Layer] = true
	}
	var promises []Promise[int32]
	for l := range layers {
		promises = append(promises, J.batchLayer(J.parts[l], addByLayer[l], rmByLayer[l]))
	}
	return NewWaitForAll[int32](promises)
}

func (J *JSONIndex) batchLayer(parts map[string]*jsonPartIndex, add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
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

	var promises []Promise[int32]
	for partPath := range paths {
		idx := parts[partPath]
		if idx == nil {
			return Fulfilled[int32](fmt.Errorf("part \"%s\" not found", partPath), 0)
		}
		promises = append(promises, idx.Batch(addByPath[partPath], rmByPath[partPath]))
	}
	return NewWaitForAll[int32](promises)
}

func (J *JSONIndex) populate(dir string, layer string) (*jsonPartIndex, error) {
	layerParts := J.parts[layer]
	if layerParts == nil {
		layerParts = make(map[string]*jsonPartIndex)
		J.parts[layer] = layerParts
	}
	idx := layerParts[dir]
	var _layer *jsonLayer
	for _, l := range J.layers {
		if l.Name == layer {
			_layer = &l
			break
		}
	}
	if _layer == nil {
		return nil, fmt.Errorf("layer \"%s\" not found", layer)
	}
	if _layer.Path == "" {
		return nil, fmt.Errorf("layer path \"%s\" not supported", _layer.URL)
	}

	if idx != nil {
		return idx, nil
	}
	idx, err := newJsonPartIndex(jsonPartIdxOpts{
		rootPath: _layer.Path,
		database: J.database,
		table:    J.table,
		partPath: dir,
		layers:   J.layers,
		layer:    layer,
	})
	if err != nil {
		return nil, err
	}
	idx.Run()
	layerParts[dir] = idx
	return idx, nil
}

func (J *JSONIndex) Get(layer string, _path string) *IndexEntry {
	dir := path.Dir(_path)
	J.lock.Lock()
	defer J.lock.Unlock()
	idx, err := J.populate(layer, dir)
	if err != nil {
		return nil
	}
	return idx.Get(layer, _path)
}

func (J *JSONIndex) Run() {
}

func (J *JSONIndex) Stop() {
	for _, l := range J.parts {
		for _, idx := range l {
			idx.Stop()
		}
	}
}

func (J *JSONIndex) RmFromDropQueue(layer string, files []string) Promise[int32] {
	filesByPath := make(map[string][]string)
	for _, file := range files {
		_path := path.Dir(file)
		filesByPath[_path] = append(filesByPath[_path], file)
	}
	J.lock.Lock()
	defer J.lock.Unlock()

	var promises []Promise[int32]
	for partPath, files := range filesByPath {
		idx, err := J.populate(layer, partPath)
		if err != nil {
			//TODO: we should do something with the error
			continue
		}
		promises = append(promises, idx.RmFromDropQueue(layer, files))
	}
	return NewWaitForAll[int32](promises)
}

func (J *JSONIndex) GetDropQueue(layer string) []string {
	var queue []string
	parts := J.parts[layer]
	if parts == nil {
		return nil
	}
	for _, idx := range parts {
		queue = append(queue, idx.GetDropQueue(layer)...)
	}
	return queue
}

func (J *JSONIndex) findHours(options QueryOptions) ([]time.Time, error) {
	var hours []time.Time
	err := filepath.Walk(path.Join(J.root, J.database, J.table), func(path string, info os.FileInfo, err error) error {
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
			sep := string(filepath.Separator)
			dirs := strings.Split(path, sep)
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
	if err != nil {
		return nil, err
	}
	var _hours []time.Time
	if options.Before.Unix() > 0 {
		for i := len(hours) - 1; i >= 0; i-- {
			if hours[i].Unix() < options.Before.Unix() {
				_hours = append(_hours, hours[i])
			}
		}
		hours = _hours
	}
	if options.After.Unix() > 0 {
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
	for _, l := range J.layers {
		if l.Path == "" {
			continue
		}
		for _, hour := range hours {
			idx, err := J.populate(l.Name, path.Join(
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
	}

	return entries, nil
}

func (J *JSONIndex) GetMovePlan(layer string) *MovePlan {
	J.lock.Lock()
	defer J.lock.Unlock()
	l := J.parts[layer]
	if l == nil {
		return nil
	}
	for _, p := range l {
		mp := p.GetMovePlanner().GetMovePlan(layer)
		if mp != nil {
			return mp
		}
	}
	return nil
}

func (J *JSONIndex) EndMove(plan *MovePlan) error {
	J.lock.Lock()
	defer J.lock.Unlock()
	l := J.parts[plan.LayerFrom]
	if l == nil {
		return fmt.Errorf("layer \"%s\" not found", plan.LayerFrom)
	}
	dir := path.Dir(plan.PathFrom)
	part := l[dir]
	if part != nil {
		return part.EndMove(plan)
	}
	return nil
}

func (J *JSONIndex) GetMovePlanner() TableMovePlanner {
	return J
}
