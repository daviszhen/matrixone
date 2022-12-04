package newplan

import (
	"go/constant"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BindExpr ->Virtual ColRef (projectTag,projects[colPos]) {UnresolvedName[0] alias, NumVal(index of bound project list)}
//
//	internals -> ProjectionBinder.BindExpr
func NewOrderBinder(projectionBinder *ProjectionBinder, selectList tree.SelectExprs) *OrderBinder {
	return &OrderBinder{
		ProjectionBinder: projectionBinder,
		selectList:       selectList,
	}
}

// BindExpr ->Virtual ColRef (projectTag,projects[colPos]) {UnresolvedName[0] alias, NumVal(index of bound project list)}
func (b *OrderBinder) BindExpr(astExpr tree.Expr) (*plan.Expr, error) {
	if colRef, ok := astExpr.(*tree.UnresolvedName); ok && colRef.NumParts == 1 {
		if colPos, ok := b.ctx.aliasMap[colRef.Parts[0]]; ok {
			return &plan.Expr{
				Typ: b.ctx.projects[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.projectTag,
						ColPos: colPos,
					},
				},
			}, nil
		}
	}

	if numVal, ok := astExpr.(*tree.NumVal); ok {
		switch numVal.Value.Kind() {
		case constant.Int:
			colPos, _ := constant.Int64Val(numVal.Value)
			if numVal.Negative() {
				colPos = -colPos
			}
			if colPos < 1 || int(colPos) > len(b.ctx.projects) {
				return nil, moerr.NewSyntaxError("ORDER BY position %v is not in select list", colPos)
			}

			colPos = colPos - 1
			return &plan.Expr{
				Typ: b.ctx.projects[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.projectTag,
						ColPos: int32(colPos),
					},
				},
			}, nil
		default:
			return nil, moerr.NewSyntaxError("non-integer constant in ORDER BY")
		}
	}

	//expand alias
	//orderBy may use the alias in project list first
	astExpr, err := b.ctx.qualifyColumnNames(astExpr, b.selectList, true)
	if err != nil {
		return nil, err
	}

	expr, err := b.ProjectionBinder.BindExpr(astExpr, 0, true)
	if err != nil {
		return nil, err
	}

	var colPos int32
	var ok bool
	exprStr := expr.String()
	if colPos, ok = b.ctx.projectByExpr[exprStr]; !ok {
		if b.ctx.isDistinct {
			return nil, moerr.NewSyntaxError("for SELECT DISTINCT, ORDER BY expressions must appear in select list")
		}

		colPos = int32(len(b.ctx.projects))
		b.ctx.projectByExpr[exprStr] = colPos
		b.ctx.projects = append(b.ctx.projects, expr)
	}

	expr = &plan.Expr{
		Typ: b.ctx.projects[colPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.ctx.projectTag,
				ColPos: colPos,
			},
		},
	}

	return expr, err
}
