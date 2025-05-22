package metadata

import (
	_ "embed"
)

//go:embed redis_scripts/patch_index.lua
var SCRIPT_PATCH_INDEX []byte

//go:embed redis_scripts/get_merge_plan.lua
var GET_MERGE_PLAN_SCRIPT []byte

//go:embed redis_scripts/end_merge.lua
var END_MERGE_SCRIPT []byte
