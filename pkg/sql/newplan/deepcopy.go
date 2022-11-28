package newplan

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func DeepCopyTyp(typ *plan.Type) *plan.Type {
	if typ == nil {
		return nil
	}
	return &plan.Type{
		Id:          typ.Id,
		NotNullable: typ.NotNullable,
		Width:       typ.Width,
		Precision:   typ.Precision,
		Size:        typ.Size,
		Scale:       typ.Scale,
		AutoIncr:    typ.AutoIncr,
	}
}

func DeepCopyExpr(expr *plan.Expr) *plan.Expr {
	if expr == nil {
		return nil
	}
	newExpr := &plan.Expr{
		Typ: DeepCopyTyp(expr.Typ),
	}

	switch item := expr.Expr.(type) {
	case *plan.Expr_C:
		pc := &plan.Const{
			Isnull: item.C.GetIsnull(),
		}

		switch c := item.C.Value.(type) {
		case *plan.Const_I8Val:
			pc.Value = &plan.Const_I8Val{I8Val: c.I8Val}
		case *plan.Const_I16Val:
			pc.Value = &plan.Const_I16Val{I16Val: c.I16Val}
		case *plan.Const_I32Val:
			pc.Value = &plan.Const_I32Val{I32Val: c.I32Val}
		case *plan.Const_I64Val:
			pc.Value = &plan.Const_I64Val{I64Val: c.I64Val}
		case *plan.Const_Dval:
			pc.Value = &plan.Const_Dval{Dval: c.Dval}
		case *plan.Const_Sval:
			pc.Value = &plan.Const_Sval{Sval: c.Sval}
		case *plan.Const_Bval:
			pc.Value = &plan.Const_Bval{Bval: c.Bval}
		case *plan.Const_U8Val:
			pc.Value = &plan.Const_U8Val{U8Val: c.U8Val}
		case *plan.Const_U16Val:
			pc.Value = &plan.Const_U16Val{U16Val: c.U16Val}
		case *plan.Const_U32Val:
			pc.Value = &plan.Const_U32Val{U32Val: c.U32Val}
		case *plan.Const_U64Val:
			pc.Value = &plan.Const_U64Val{U64Val: c.U64Val}
		case *plan.Const_Fval:
			pc.Value = &plan.Const_Fval{Fval: c.Fval}
		case *plan.Const_Dateval:
			pc.Value = &plan.Const_Dateval{Dateval: c.Dateval}
		case *plan.Const_Timeval:
			pc.Value = &plan.Const_Timeval{Timeval: c.Timeval}
		case *plan.Const_Datetimeval:
			pc.Value = &plan.Const_Datetimeval{Datetimeval: c.Datetimeval}
		case *plan.Const_Decimal64Val:
			pc.Value = &plan.Const_Decimal64Val{Decimal64Val: &plan.Decimal64{A: c.Decimal64Val.A}}
		case *plan.Const_Decimal128Val:
			pc.Value = &plan.Const_Decimal128Val{Decimal128Val: &plan.Decimal128{A: c.Decimal128Val.A, B: c.Decimal128Val.B}}
		case *plan.Const_Timestampval:
			pc.Value = &plan.Const_Timestampval{Timestampval: c.Timestampval}
		case *plan.Const_Jsonval:
			pc.Value = &plan.Const_Jsonval{Jsonval: c.Jsonval}
		case *plan.Const_Defaultval:
			pc.Value = &plan.Const_Defaultval{Defaultval: c.Defaultval}
		case *plan.Const_UpdateVal:
			pc.Value = &plan.Const_UpdateVal{UpdateVal: c.UpdateVal}
		}

		newExpr.Expr = &plan.Expr_C{
			C: pc,
		}

	case *plan.Expr_P:
		newExpr.Expr = &plan.Expr_P{
			P: &plan.ParamRef{
				Pos: item.P.GetPos(),
			},
		}

	case *plan.Expr_V:
		newExpr.Expr = &plan.Expr_V{
			V: &plan.VarRef{
				Name: item.V.GetName(),
			},
		}

	case *plan.Expr_Col:
		newExpr.Expr = &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: item.Col.GetRelPos(),
				ColPos: item.Col.GetColPos(),
				Name:   item.Col.GetName(),
			},
		}

	case *plan.Expr_F:
		newArgs := make([]*plan.Expr, len(item.F.Args))
		for idx, arg := range item.F.Args {
			newArgs[idx] = DeepCopyExpr(arg)
		}
		newExpr.Expr = &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Server:     item.F.Func.GetServer(),
					Db:         item.F.Func.GetDb(),
					Schema:     item.F.Func.GetSchema(),
					Obj:        item.F.Func.GetObj(),
					ServerName: item.F.Func.GetServerName(),
					DbName:     item.F.Func.GetDbName(),
					SchemaName: item.F.Func.GetSchemaName(),
					ObjName:    item.F.Func.GetObjName(),
				},
				Args: newArgs,
			},
		}

	case *plan.Expr_Sub:
		newExpr.Expr = &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId: item.Sub.GetNodeId(),
			},
		}

	case *plan.Expr_Corr:
		newExpr.Expr = &plan.Expr_Corr{
			Corr: &plan.CorrColRef{
				ColPos: item.Corr.GetColPos(),
				RelPos: item.Corr.GetRelPos(),
				Depth:  item.Corr.GetDepth(),
			},
		}

	case *plan.Expr_T:
		newExpr.Expr = &plan.Expr_T{
			T: &plan.TargetType{
				Typ: DeepCopyTyp(item.T.Typ),
			},
		}

	case *plan.Expr_Max:
		newExpr.Expr = &plan.Expr_Max{
			Max: &plan.MaxValue{
				Value: item.Max.GetValue(),
			},
		}

	case *plan.Expr_List:
		e := &plan.ExprList{
			List: make([]*plan.Expr, len(item.List.List)),
		}
		for i, ie := range item.List.List {
			e.List[i] = DeepCopyExpr(ie)
		}
		newExpr.Expr = &plan.Expr_List{
			List: e,
		}
	}

	return newExpr
}
