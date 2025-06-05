package metadata

import (
	"time"
)

func (J *jsonPartIndex) GetMovePlan(writerId string, layer string) (MovePlan, error) {

	J.m.Lock()
	defer J.m.Unlock()
	var plan *MovePlan
	J.entries.Range(func(key, value any) bool {
		val := value.(*jsonIndexEntry)
		if J.filesInMerge[val.Path] {
			return true
		}
		layerTdx := J.getLayer(val.Layer)
		if layerTdx < 0 {
			return true
		}
		layerTo := ""
		if layerTdx+1 < len(J.layers) {
			layerTo = J.layers[layerTdx+1].Name
		}
		if J.layers[layerTdx].TTLSec > 0 &&
			time.Now().UnixNano()-val.ChunkTime >= int64(J.layers[layerTdx].TTLSec)*1000000000 {
			plan = &MovePlan{
				ID:        "",
				Database:  J.database,
				Table:     J.table,
				PathFrom:  val.Path,
				LayerFrom: val.Layer,
				PathTo:    val.Path,
				LayerTo:   layerTo,
			}
			return false
		}
		return true
	})
	return *plan, nil
}

func (J *jsonPartIndex) EndMove(plan MovePlan) Promise[int32] {
	J.m.Lock()
	defer J.m.Unlock()
	if _, ok := J.filesInMove[plan.PathFrom]; !ok {
		return Fulfilled[int32](nil, 0)
	}
	delete(J.filesInMove, plan.PathFrom)
	p := NewPromise[int32]()
	J.promises = append(J.promises, p)
	J.doUpdate()
	return p
}

func (J *jsonPartIndex) GetMovePlanner() TableMovePlanner {
	return J
}
