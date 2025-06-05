package metadata

import "path"

func (J *JSONIndex) GetDropQueue(writerId string, layer string) (DropPlan, error) {
	parts := J.parts[layer]
	if parts == nil {
		return DropPlan{}, nil
	}
	for _, idx := range parts {
		p, err := idx.GetDropPlanner().GetDropQueue(writerId, layer)
		if err != nil {
			return DropPlan{}, err
		}
		if p.Path != "" {
			return p, nil
		}
	}
	return DropPlan{}, nil
}

func (J *JSONIndex) GetDropPlanner() TableDropPlanner {
	return J
}

func (J *JSONIndex) RmFromDropQueue(plan DropPlan) Promise[int32] {
	l := J.parts[plan.Layer]
	if l == nil {
		return Fulfilled(nil, int32(0))
	}
	dir := path.Dir(plan.Path)
	part := l[dir]
	if part != nil {
		return part.RmFromDropQueue(plan)
	}
	return Fulfilled(nil, int32(0))
}
