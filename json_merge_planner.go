package metadata

import "path"

func (J *JSONIndex) GetMergePlan(writerId string, layer string, iteration int) (MergePlan, error) {
	J.lock.Lock()
	defer J.lock.Unlock()
	parts, ok := J.parts[layer]
	if !ok {
		return MergePlan{}, nil
	}
	for _, part := range parts {
		plan, err := part.GetMergePlan(writerId, layer, iteration)
		if err != nil {
			return MergePlan{}, err
		}
		if len(plan.From) != 0 {
			return plan, nil
		}

	}
	return MergePlan{}, nil
}

func (J *JSONIndex) EndMerge(plan MergePlan) Promise[int32] {
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
