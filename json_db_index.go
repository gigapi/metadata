package metadata

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type jsonDBIndex struct {
	root string
}

/*func NewJSONDBIndex(root string) DBIndex {
	return &jsonDBIndex{
		root: root,
	}
}*/

func (j *jsonDBIndex) Databases() ([]string, error) {
	ents, err := os.ReadDir(j.root)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, ent := range ents {
		if ent.IsDir() {
			res = append(res, ent.Name())
		}
	}
	return res, nil
}

func (j *jsonDBIndex) Tables(database string) ([]string, error) {
	ents, err := os.ReadDir(path.Join(j.root, database))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var res []string
	for _, ent := range ents {
		if ent.IsDir() {
			res = append(res, ent.Name())
		}
	}
	return res, nil
}

func (j *jsonDBIndex) Paths(database string, table string) []string {
	root := path.Join(j.root, database, table)
	var res []string
	filepath.Walk(path.Join(j.root, database, table), func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}
		if strings.HasPrefix(info.Name(), "hour=") {
			res = append(res, path[len(root):])
			return filepath.SkipDir
		}
		return nil
	})
	return res
}
