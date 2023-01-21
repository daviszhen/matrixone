package newplan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/smartystreets/goconvey/convey"
)

func getJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "  ")
	if err != nil {
		panic(err)
	}
	return out.Bytes()
}

func runCase(sql string) (bool, error) {
	var plan1, plan2 *plan.Plan
	var err error
	var one, two tree.Statement
	one, err = parsers.ParseOne(dialect.MYSQL, sql)
	if err != nil {
		return false, err
	}
	cc := sqlplan.NewMockCompilerContext()
	plan1, err = BuildPlan(cc, one)
	if err != nil {
		return false, err
	}
	fmt.Println("plan1", plan1.String())
	err = os.WriteFile("plan1.json", getJSON(plan1), 0777)
	if err != nil {
		return false, err
	}

	cc1 := sqlplan.NewMockCompilerContext()
	two, err = parsers.ParseOne(dialect.MYSQL, sql)
	if err != nil {
		return false, err
	}
	plan2, err = sqlplan.BuildPlan(cc1, two)
	if err != nil {
		return false, err
	}
	fmt.Println()
	fmt.Println("plan2", plan2.String())
	err = os.WriteFile("plan2.json", getJSON(plan2), 0777)
	if err != nil {
		return false, err
	}
	return plan1.String() == plan2.String(), nil
}

func Test_build(t *testing.T) {
	convey.Convey("t", t, func() {
		sql := `select l_returnflag from lineitem;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build1(t *testing.T) {
	convey.Convey("t2", t, func() {
		sql := `select l_returnflag,l_linestatus,l_quantity,l_extendedprice,l_quantity,l_discount,l_tax from lineitem;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build2(t *testing.T) {
	convey.Convey("t3", t, func() {
		sql := `select l_returnflag as a,l_linestatus as b,l_quantity as b,l_extendedprice as c,l_quantity as d,l_discount as e,l_tax as f from lineitem;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build3(t *testing.T) {
	convey.Convey("t4", t, func() {
		sql := `select l_extendedprice * (1 - l_discount) from lineitem;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build4(t *testing.T) {
	convey.Convey("t5", t, func() {
		sql := `select l_extendedprice * (1 - l_discount) * (1 + l_tax) from lineitem;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build5(t *testing.T) {
	convey.Convey("t6", t, func() {
		sql := `select l_extendedprice * (1 - l_discount) * (1 + l_tax) 
				from lineitem 
                order by
					l_returnflag,
					l_linestatus;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build6(t *testing.T) {
	convey.Convey("t7", t, func() {
		sql := `select
					l_returnflag,
					l_linestatus,
    				sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge 
				from lineitem 
				group by
					l_returnflag,
					l_linestatus
                order by
					l_returnflag,
					l_linestatus;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build8(t *testing.T) {
	convey.Convey("t8", t, func() {
		sql := `select
					l_returnflag,
					l_linestatus,
					sum(l_quantity) as sum_qty,
					sum(l_extendedprice) as sum_base_price,
					sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    				sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
					avg(l_quantity) as avg_qty,
					avg(l_extendedprice) as avg_price,
					avg(l_discount) as avg_disc,
					count(*) as count_order
				from lineitem 
				group by
					l_returnflag,
					l_linestatus
                order by
					l_returnflag,
					l_linestatus;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build9(t *testing.T) {
	convey.Convey("tpch-q1", t, func() {
		sql := `select
					l_returnflag,
					l_linestatus,
					sum(l_quantity) as sum_qty,
					sum(l_extendedprice) as sum_base_price,
					sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
					sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
					avg(l_quantity) as avg_qty,
					avg(l_extendedprice) as avg_price,
					avg(l_discount) as avg_disc,
					count(*) as count_order
				from
					lineitem
				where
					l_shipdate <= date '1998-12-01' - interval 112 day
				group by
					l_returnflag,
					l_linestatus
				order by
					l_returnflag,
					l_linestatus
				;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build10(t *testing.T) {
	convey.Convey("tpch-q3", t, func() {
		sql := `select
				l_orderkey,
				sum(l_extendedprice * (1 - l_discount)) as revenue,
				o_orderdate,
				o_shippriority
			from
				customer,
				orders,
				lineitem
			where
				c_mktsegment = 'HOUSEHOLD'
				and c_custkey = o_custkey
				and l_orderkey = o_orderkey
				and o_orderdate < date '1995-03-29'
				and l_shipdate > date '1995-03-29'
			group by
				l_orderkey,
				o_orderdate,
				o_shippriority
			order by
				revenue desc,
				o_orderdate
			limit 10
			;`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build12(t *testing.T) {
	convey.Convey("tpch-q10", t, func() {
		sql := `select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-03-01'
	and o_orderdate < date '1993-03-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20
;

`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_Debug(t *testing.T) {
	cc := sqlplan.NewMockCompilerContext()
	sql := `select
				l_orderkey,
				sum(l_extendedprice * (1 - l_discount)) as revenue,
				o_orderdate,
				o_shippriority
			from
				customer,
				orders,
				lineitem
			where
				c_mktsegment = 'HOUSEHOLD'
				and c_custkey = o_custkey
				and l_orderkey = o_orderkey
				and o_orderdate < date '1995-03-29'
				and l_shipdate > date '1995-03-29'
			group by
				l_orderkey,
				o_orderdate,
				o_shippriority
			order by
				revenue desc,
				o_orderdate
			limit 10
			;`
	var one tree.Statement
	var err error
	var plan3 *plan.Plan
	one, err = parsers.ParseOne(dialect.MYSQL, sql)
	if err != nil {
		return
	}
	plan3, err = sqlplan.BuildPlan(cc, one)
	if err != nil {
		return
	}
	fmt.Println("plan3", plan3.String())
	err = os.WriteFile("plan3.json", getJSON(plan3), 0777)
	if err != nil {
		return
	}
}
