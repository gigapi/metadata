package metadata

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type jsonDBIndex struct {
	layers []jsonLayer
}

func NewJSONDBIndex(layers []Layer) DBIndex {
	jLayers := make([]jsonLayer, len(layers))
	for i, layer := range layers {
		jLayers[i] = layer2JsonLayer(layer)
	}
	return &jsonDBIndex{
		layers: jLayers,
	}
}

func (j *jsonDBIndex) Databases() ([]string, error) {
	res := map[string]bool{}
	for _, l := range j.layers {
		ents, err := os.ReadDir(path.Join(l.Path))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, ent := range ents {
			if ent.IsDir() {
				res[ent.Name()] = true
			}
		}
	}
	_res := make([]string, 0, len(res))
	for k := range res {
		_res = append(_res, k)
	}
	return _res, nil
}

func (j *jsonDBIndex) Tables(database string) ([]string, error) {
	res := map[string]bool{}
	for _, l := range j.layers {
		ents, err := os.ReadDir(path.Join(l.Path, database))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, ent := range ents {
			if ent.IsDir() {
				res[ent.Name()] = true
			}
		}
	}
	_res := make([]string, 0, len(res))
	for k := range res {
		_res = append(_res, k)
	}
	return _res, nil
}

func (j *jsonDBIndex) Paths(database string, table string) ([]string, error) {
	res := map[string]bool{}
	for _, l := range j.layers {
		root := path.Join(l.Path, database, table)
		filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				return nil
			}
			if strings.HasPrefix(info.Name(), "hour=") {
				res[path[len(root)+1:]] = true
				return filepath.SkipDir
			}
			return nil
		})
	}
	_res := make([]string, 0, len(res))
	for k := range res {
		_res = append(_res, k)
	}
	return _res, nil
}
