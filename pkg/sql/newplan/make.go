package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func makePlan2Type(typ *types.Type) *plan.Type {
	return &plan.Type{
		Id:        int32(typ.Oid),
		Width:     typ.Width,
		Precision: typ.Precision,
		Size:      typ.Size,
		Scale:     typ.Scale,
	}
}
