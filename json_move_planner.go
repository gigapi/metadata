package metadata

import (
	"fmt"
	"path"
)

func (J *JSONIndex) GetMovePlan(writerId string, layer string) (MovePlan, error) {
	J.lock.Lock()
	defer J.lock.Unlock()
	l := J.parts[layer]
	if l == nil {
		return MovePlan{}, nil
	}
	for _, p := range l {
		mp, err := p.GetMovePlanner().GetMovePlan(writerId, layer)
		if err != nil {
			return MovePlan{}, err
		}
		if mp.PathFrom != "" {
			return mp, nil
		}
	}
	return MovePlan{}, nil
}

func (J *JSONIndex) EndMove(plan MovePlan) Promise[int32] {
	J.lock.Lock()
	defer J.lock.Unlock()
	l := J.parts[plan.LayerFrom]
	if l == nil {
		return Fulfilled(fmt.Errorf("layer \"%s\" not found", plan.LayerFrom), int32(0))
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
