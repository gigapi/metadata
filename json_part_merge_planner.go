package metadata

import (
	"fmt"
	"github.com/google/uuid"
	"path"
	"strings"
	"time"
)

func (J *jsonPartIndex) GetMergePlan(writerId string, layer string, iteration int) (MergePlan, error) {
	suffix := fmt.Sprintf(".%d.parquet", iteration)
	var from []string
	var size int64
	if iteration > len(MergeConfigurations) {
		return MergePlan{}, fmt.Errorf("no more merge configurations available for iteration %d", iteration)
	}
	conf := MergeConfigurations[iteration-1]
	now := time.Now()
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
		if entry.ChunkTime+conf.TimeoutSec()*1000000000 >= now.UnixNano() {
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

	tablePath := path.Join(J.rootPath, J.database, J.table, "data") + "/"
	partPath := J.idxPath[len(tablePath):]
	return MergePlan{
		Layer:     layer,
		Database:  J.database,
		Table:     J.table,
		From:      from,
		To:        path.Join(partPath, fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
		Iteration: iteration,
	}, nil
}

func (J *jsonPartIndex) EndMerge(plan MergePlan) Promise[int32] {
	J.m.Lock()
	defer J.m.Unlock()
	update := false
	for _, file := range plan.From {
		update = update || J.filesInMerge[file]
		delete(J.filesInMerge, file)
	}
	if !update {
		return Fulfilled[int32](nil, 0)
	}
	p := NewPromise[int32]()
	J.promises = append(J.promises, p)
	return p
}

func (J *jsonPartIndex) GetMergePlanner() TableMergePlanner {
	return J
}
