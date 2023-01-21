package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"math"
)

const (
	JoinSideNone       int8 = 0
	JoinSideLeft            = 1 << iota
	JoinSideRight           = 1 << iota
	JoinSideBoth            = JoinSideLeft | JoinSideRight
	JoinSideCorrelated      = 1 << iota
)

func increaseRefCnt(expr *plan.Expr, colRefCnt map[[2]int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefCnt[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]++

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			increaseRefCnt(arg, colRefCnt)
		}
	}
}

func decreaseRefCnt(expr *plan.Expr, colRefCnt map[[2]int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefCnt[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]--

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			decreaseRefCnt(arg, colRefCnt)
		}
	}
}

func replaceColRefs(expr *plan.Expr, tag int32, projects []*plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = replaceColRefs(arg, tag, projects)
		}
	case *plan.Expr_Col:
		colRef := exprImpl.Col
		if colRef.RelPos == tag {
			expr = DeepCopyExpr(projects[colRef.ColPos])
		}
	}
	return expr
}

// checkNoNeedCast
// if constant's type higher than column's type
// and constant's value in range of column's type, then no cast was needed
func checkNoNeedCast(constT, columnT types.T, constExpr *plan.Expr_C) bool {
	key := [2]types.T{columnT, constT}
	// lowIntCol > highIntConst
	if _, ok := intCastTableForRewrite[key]; ok {
		val, valOk := constExpr.C.Value.(*plan.Const_I64Val)
		if !valOk {
			return false
		}
		constVal := val.I64Val

		switch columnT {
		case types.T_int8:
			return constVal <= int64(math.MaxInt8) && constVal >= int64(math.MinInt8)
		case types.T_int16:
			return constVal <= int64(math.MaxInt16) && constVal >= int64(math.MinInt16)
		case types.T_int32:
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		}
	}

	// lowUIntCol > highUIntConst
	if _, ok := uintCastTableForRewrite[key]; ok {
		val, valOk := constExpr.C.Value.(*plan.Const_U64Val)
		if !valOk {
			return false
		}
		constVal := val.U64Val

		switch columnT {
		case types.T_uint8:
			return constVal <= uint64(math.MaxUint8)
		case types.T_uint16:
			return constVal <= uint64(math.MaxUint16)
		case types.T_uint32:
			return constVal <= uint64(math.MaxUint32)
		}
	}

	// lowUIntCol > highIntConst
	if _, ok := uint2intCastTableForRewrite[key]; ok {
		val, valOk := constExpr.C.Value.(*plan.Const_I64Val)
		if !valOk {
			return false
		}
		constVal := val.I64Val

		switch columnT {
		case types.T_uint8:
			return constVal <= int64(math.MaxUint8)
		case types.T_uint16:
			return constVal <= int64(math.MaxUint16)
		case types.T_uint32:
			return constVal <= int64(math.MaxUint32)
		}
	}

	return false
}

func containsTag(expr *plan.Expr, tag int32) bool {
	var ret bool

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			ret = ret || containsTag(arg, tag)
		}

	case *plan.Expr_Col:
		return exprImpl.Col.RelPos == tag
	}

	return ret
}

func combinePlanConjunction(exprs []*plan.Expr) (expr *plan.Expr, err error) {
	expr = exprs[0]

	for i := 1; i < len(exprs); i++ {
		expr, err = bindFuncExprImplByPlanExpr("and", []*plan.Expr{expr, exprs[i]})

		if err != nil {
			break
		}
	}

	return
}

// applyDistributivity (X AND B) OR (X AND C) OR (X AND D) => X AND (B OR C OR D)
// TODO: move it into optimizer
func applyDistributivity(expr *plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = applyDistributivity(arg)
		}

		if exprImpl.F.Func.ObjName != "or" {
			break
		}

		leftConds := splitPlanConjunction(exprImpl.F.Args[0])
		rightConds := splitPlanConjunction(exprImpl.F.Args[1])

		condMap := make(map[string]int)

		for _, cond := range rightConds {
			condMap[cond.String()] = JoinSideRight
		}

		var commonConds, leftOnlyConds, rightOnlyConds []*plan.Expr

		for _, cond := range leftConds {
			exprStr := cond.String()

			if condMap[exprStr] == JoinSideRight {
				commonConds = append(commonConds, cond)
				condMap[exprStr] = JoinSideBoth
			} else {
				leftOnlyConds = append(leftOnlyConds, cond)
				condMap[exprStr] = JoinSideLeft
			}
		}

		for _, cond := range rightConds {
			if condMap[cond.String()] == JoinSideRight {
				rightOnlyConds = append(rightOnlyConds, cond)
			}
		}

		if len(commonConds) == 0 {
			return expr
		}

		expr, _ = combinePlanConjunction(commonConds)

		if len(leftOnlyConds) == 0 || len(rightOnlyConds) == 0 {
			return expr
		}

		leftExpr, _ := combinePlanConjunction(leftOnlyConds)
		rightExpr, _ := combinePlanConjunction(rightOnlyConds)

		leftExpr, _ = bindFuncExprImplByPlanExpr("or", []*plan.Expr{leftExpr, rightExpr})

		expr, _ = bindFuncExprImplByPlanExpr("and", []*plan.Expr{expr, leftExpr})
	}

	return expr
}

func unionSlice(left, right []string) []string {
	if len(left) < 1 {
		return right
	}
	if len(right) < 1 {
		return left
	}
	m := make(map[string]bool, len(left)+len(right))
	for _, s := range left {
		m[s] = true
	}
	for _, s := range right {
		m[s] = true
	}
	ret := make([]string, 0)
	for s := range m {
		ret = append(ret, s)
	}
	return ret
}

func intersectSlice(left, right []string) []string {
	if len(left) < 1 || len(right) < 1 {
		return left
	}
	m := make(map[string]bool, len(left)+len(right))
	for _, s := range left {
		m[s] = true
	}
	ret := make([]string, 0)
	for _, s := range right {
		if _, ok := m[s]; ok {
			ret = append(ret, s)
		}
	}
	return ret
}

/*
DNF means disjunctive normal form, for example (a and b) or (c and d) or (e and f)
if we have a DNF filter, for example (c1=1 and c2=1) or (c1=2 and c2=2)
we can have extra filter: (c1=1 or c1=2) and (c2=1 or c2=2), which can be pushed down to optimize join

(c1=1 or c1=2) and (c2=1 or c2=2)
c1=1 and c2=1 *
or
c1=1 and c2=2
or
c1=2 and c2=1
or
c1=2 and c2=2 *


checkDNF scan the expr and return all groups of cond
for example (c1=1 and c2=1) or (c1=2 and c3=2), c1 is a group because it appears in all disjunctives
and c2,c3 is not a group

walkThroughDNF accept a keyword string, walk through the expr,
and extract all the conds which contains the keyword
*/
//(c1=1 and c2=1) or (c1=2 and c3=2)
func checkDNF(expr *plan.Expr) []string {
	var ret []string
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "or" {
			//step1 : -> c1
			//(c1=1 and c2=1) -> c1,c2
			//(c1=2 and c3=2) -> c1,c3
			left := checkDNF(exprImpl.F.Args[0])
			right := checkDNF(exprImpl.F.Args[1])
			return intersectSlice(left, right)
		}

		for _, arg := range exprImpl.F.Args {
			//step2 :
			//(c1=1 and c2=1)
			//	step3: -> c1, c2
			//	c1=1 -> c1
			//	c2=1 -> c2
			//(c1=2 and c3=2)
			//	step3: -> c1, c3
			//	c1=2 -> c1
			//	c3=2 -> c3
			ret = unionSlice(ret, checkDNF(arg))
		}
		return ret

	case *plan.Expr_Corr:
		ret = append(ret, exprImpl.Corr.String())
	case *plan.Expr_Col:
		ret = append(ret, exprImpl.Col.String())
	}
	return ret
}

// (c1=1 and c2=1) or (c1=2 and c2=2)
// kewords : c1
func walkThroughDNF(expr *plan.Expr, keywords string) *plan.Expr {
	var retExpr *plan.Expr
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "or" {
			left := walkThroughDNF(exprImpl.F.Args[0], keywords)
			right := walkThroughDNF(exprImpl.F.Args[1], keywords)
			if left != nil && right != nil {
				//return c1=1 or c1=2
				retExpr, _ = bindFuncExprImplByPlanExpr("or", []*plan.Expr{left, right})
				return retExpr
			}
		} else if exprImpl.F.Func.ObjName == "and" {
			//step2: (c1=1 and c2=1) ->c1=1
			//step2: (c1=2 and c2=2) ->c1=2
			left := walkThroughDNF(exprImpl.F.Args[0], keywords)
			right := walkThroughDNF(exprImpl.F.Args[1], keywords)
			if left == nil {
				return right
			} else if right == nil {
				return left
			} else {
				retExpr, _ = bindFuncExprImplByPlanExpr("and", []*plan.Expr{left, right})
				return retExpr
			}
		} else {
			//step3:
			//c1=1 : -> c1=1
			//	c1 -> c1
			//	1 -> 1
			//c2=1: nil
			//	c2 -> nil
			//c1=2: c1=2
			//	c1 -> c1
			//	2 -> 2
			//c2=2: nil
			//	c2->nil
			for _, arg := range exprImpl.F.Args {
				if walkThroughDNF(arg, keywords) == nil {
					return nil
				}
			}
			return expr
		}

	case *plan.Expr_Corr:
		if exprImpl.Corr.String() == keywords {
			return expr
		} else {
			return nil
		}
	case *plan.Expr_Col:
		if exprImpl.Col.String() == keywords {
			return expr
		} else {
			return nil
		}
	}
	return expr
}

func splitPlanConjunction(expr *plan.Expr) []*plan.Expr {
	var exprs []*plan.Expr
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "and" {
			exprs = append(exprs, splitPlanConjunction(exprImpl.F.Args[0])...)
			exprs = append(exprs, splitPlanConjunction(exprImpl.F.Args[1])...)
		} else {
			exprs = append(exprs, expr)
		}

	default:
		exprs = append(exprs, expr)
	}

	return exprs
}

func getJoinSide(expr *plan.Expr, leftTags, rightTags map[int32]*Binding) (side int8) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= getJoinSide(arg, leftTags, rightTags)
		}

	case *plan.Expr_Col:
		if _, ok := leftTags[exprImpl.Col.RelPos]; ok {
			side = JoinSideLeft
		} else if _, ok := rightTags[exprImpl.Col.RelPos]; ok {
			side = JoinSideRight
		}

	case *plan.Expr_Corr:
		side = JoinSideCorrelated
	}

	return
}

func replaceColRefWithNull(expr *plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		expr = &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: true,
				},
			},
		}

	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = replaceColRefWithNull(arg)
		}
	}

	return expr
}

func rejectsNull(filter *plan.Expr) bool {
	filter = replaceColRefWithNull(DeepCopyExpr(filter))

	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	filter, err := ConstantFold(bat, filter)
	if err != nil {
		return false
	}

	if f, ok := filter.Expr.(*plan.Expr_C); ok {
		if f.C.Isnull {
			return true
		}

		if fbool, ok := f.C.Value.(*plan.Const_Bval); ok {
			return !fbool.Bval
		}
	}

	return false
}

func ConstantFold(bat *batch.Batch, e *plan.Expr) (*plan.Expr, error) {
	var err error

	ef, ok := e.Expr.(*plan.Expr_F)
	if !ok {
		return e, nil
	}
	overloadID := ef.F.Func.GetObj()
	f, err := function.GetFunctionByID(overloadID)
	if err != nil {
		return nil, err
	}
	if f.Volatile { // function cannot be fold
		return e, nil
	}
	for i := range ef.F.Args {
		ef.F.Args[i], err = ConstantFold(bat, ef.F.Args[i])
		if err != nil {
			return nil, err
		}
	}
	if !isConstant(e) {
		return e, nil
	}
	// XXX MPOOL
	// This is a bug -- colexec EvalExpr need to eval, therefore, could potentially need
	// a mpool.  proc is passed in a nil, where do I get a mpool?   Session?
	vec, err := colexec.EvalExpr(bat, nil, e)
	if err != nil {
		return nil, err
	}
	c := getConstantValue(vec)
	if c == nil {
		return e, nil
	}
	ec := &plan.Expr_C{
		C: c,
	}
	e.Expr = ec
	return e, nil
}

func getConstantValue(vec *vector.Vector) *plan.Const {
	if nulls.Any(vec.Nsp) {
		return &plan.Const{Isnull: true}
	}
	switch vec.Typ.Oid {
	case types.T_bool:
		return &plan.Const{
			Value: &plan.Const_Bval{
				Bval: vec.Col.([]bool)[0],
			},
		}
	case types.T_int8:
		return &plan.Const{
			Value: &plan.Const_I8Val{
				I8Val: int32(vec.Col.([]int8)[0]),
			},
		}
	case types.T_int16:
		return &plan.Const{
			Value: &plan.Const_I16Val{
				I16Val: int32(vec.Col.([]int16)[0]),
			},
		}
	case types.T_int32:
		return &plan.Const{
			Value: &plan.Const_I32Val{
				I32Val: vec.Col.([]int32)[0],
			},
		}
	case types.T_int64:
		return &plan.Const{
			Value: &plan.Const_I64Val{
				I64Val: vec.Col.([]int64)[0],
			},
		}
	case types.T_uint8:
		return &plan.Const{
			Value: &plan.Const_U8Val{
				U8Val: uint32(vec.Col.([]uint8)[0]),
			},
		}
	case types.T_uint16:
		return &plan.Const{
			Value: &plan.Const_U16Val{
				U16Val: uint32(vec.Col.([]uint16)[0]),
			},
		}
	case types.T_uint32:
		return &plan.Const{
			Value: &plan.Const_U32Val{
				U32Val: vec.Col.([]uint32)[0],
			},
		}
	case types.T_uint64:
		return &plan.Const{
			Value: &plan.Const_U64Val{
				U64Val: vec.Col.([]uint64)[0],
			},
		}
	case types.T_float64:
		return &plan.Const{
			Value: &plan.Const_Dval{
				Dval: vec.Col.([]float64)[0],
			},
		}
	case types.T_varchar:
		return &plan.Const{
			Value: &plan.Const_Sval{
				Sval: vec.GetString(0),
			},
		}
	default:
		return nil
	}
}

func isConstant(e *plan.Expr) bool {
	switch ef := e.Expr.(type) {
	case *plan.Expr_C, *plan.Expr_T:
		return true
	case *plan.Expr_F:
		overloadID := ef.F.Func.GetObj()
		f, err := function.GetFunctionByID(overloadID)
		if err != nil {
			return false
		}
		if f.Volatile { // function cannot be fold
			return false
		}
		for i := range ef.F.Args {
			if !isConstant(ef.F.Args[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func getHyperEdgeFromExpr(expr *plan.Expr, leafByTag map[int32]int32, hyperEdge map[int32]any) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		hyperEdge[leafByTag[exprImpl.Col.RelPos]] = nil

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			getHyperEdgeFromExpr(arg, leafByTag, hyperEdge)
		}
	}
}
