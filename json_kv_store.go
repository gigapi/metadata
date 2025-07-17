package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

type jsonKVStoreIndex struct {
	Path string

	cache        map[string][]byte
	saveCtx      context.Context
	planSave     func()
	ctx          context.Context
	destroy      func()
	m            sync.Mutex
	savePromises []Promise[int32]
}

func NewJSONKVStoreIndex(path string) (KVStoreIndex, error) {
	j := &jsonKVStoreIndex{
		Path:  path,
		cache: make(map[string][]byte),
	}
	j.saveCtx, j.planSave = context.WithCancel(context.Background())
	j.ctx, j.destroy = context.WithCancel(context.Background())

	ok, err := j.Load()
	if err != nil {
		return nil, err
	}
	if !ok {
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		_, err = f.WriteString("{}")
		if err != nil {
			return nil, err
		}
	}

	go func() {
		for {
			select {
			case <-j.saveCtx.Done():
				j.doSave()
			case <-j.ctx.Done():
				return
			}
		}
	}()

	return j, nil
}

func (j *jsonKVStoreIndex) Load() (bool, error) {
	f, err := os.Open(j.Path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	data, err := os.ReadFile(j.Path)
	if err != nil {
		return false, err
	}

	err = json.Unmarshal(data, &j.cache)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (j *jsonKVStoreIndex) Destroy() {
	j.destroy()
}

func (j *jsonKVStoreIndex) Get(key string) ([]byte, error) {
	j.m.Lock()
	defer j.m.Unlock()

	if val, ok := j.cache[key]; ok {
		return val, nil
	}
	return nil, nil
}

func (j *jsonKVStoreIndex) Put(key string, value []byte) error {
	j.m.Lock()
	j.cache[key] = value
	p := NewPromise[int32]()
	j.savePromises = append(j.savePromises, p)
	j.planSave()
	j.m.Unlock()
	_, err := p.Get()
	return err
}

func (j *jsonKVStoreIndex) Delete(key string) error {
	j.m.Lock()
	delete(j.cache, key)
	p := NewPromise[int32]()
	j.savePromises = append(j.savePromises, p)
	j.planSave()
	j.m.Unlock()
	_, err := p.Get()
	return err
}

func (j *jsonKVStoreIndex) doSave() {
	j.m.Lock()
	cache := j.cache
	promises := j.savePromises
	j.savePromises = nil
	j.saveCtx, j.planSave = context.WithCancel(context.Background())
	j.m.Unlock()
	release := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}
	contents, err := json.Marshal(cache)
	if err != nil {
		release(err)
		return
	}

	f, err := os.Create(j.Path + ".tmp")
	if err != nil {
		release(err)
		return
	}
	defer f.Close()

	_, err = f.Write(contents)
	if err != nil {
		release(err)
		return
	}
	err = os.Rename(j.Path+".tmp", j.Path)
	release(err)
}
