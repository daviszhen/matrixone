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
	fmt.Println("newplan1", plan1.String())
	err = os.WriteFile("newplan1.json", getJSON(plan1), 0777)
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
	fmt.Println("oldplan2", plan2.String())
	err = os.WriteFile("oldplan2.json", getJSON(plan2), 0777)
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

func Test_build9_q1(t *testing.T) {
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

func Test_build10_q3(t *testing.T) {
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

func Test_build11_q5(t *testing.T) {
	convey.Convey("tpch-q5", t, func() {
		sql := `select
					n_name,
					sum(l_extendedprice * (1 - l_discount)) as revenue
				from
					customer,
					orders,
					lineitem,
					supplier,
					nation,
					region
				where
					c_custkey = o_custkey
					and l_orderkey = o_orderkey
					and l_suppkey = s_suppkey
					and c_nationkey = s_nationkey
					and s_nationkey = n_nationkey
					and n_regionkey = r_regionkey
					and r_name = 'AMERICA'
					and o_orderdate >= date '1994-01-01'
					and o_orderdate < date '1994-01-01' + interval '1' year
				group by
					n_name
				order by
					revenue desc
				;
				`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build12_q10(t *testing.T) {
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

func Test_build13_q2(t *testing.T) {
	convey.Convey("tpch-q2", t, func() {
		sql := `select
					s_acctbal,
					s_name,
					n_name,
					p_partkey,
					p_mfgr,
					s_address,
					s_phone,
					s_comment
				from
					part,
					supplier,
					partsupp,
					nation,
					region
				where
					p_partkey = ps_partkey
					and s_suppkey = ps_suppkey
					and p_size = 48
					and p_type like '%TIN'
					and s_nationkey = n_nationkey
					and n_regionkey = r_regionkey
					and r_name = 'MIDDLE EAST'
					and ps_supplycost = (
						select
							min(ps_supplycost)
						from
							partsupp,
							supplier,
							nation,
							region
						where
							p_partkey = ps_partkey
							and s_suppkey = ps_suppkey
							and s_nationkey = n_nationkey
							and n_regionkey = r_regionkey
							and r_name = 'MIDDLE EAST'
					)
				order by
					s_acctbal desc,
					n_name,
					s_name,
					p_partkey
				limit 100
				;
				`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build14_q4(t *testing.T) {
	convey.Convey("tpch-q4", t, func() {
		sql := `select
					o_orderpriority,
					count(*) as order_count
				from
					orders
				where
					o_orderdate >= date '1997-07-01'
					and o_orderdate < date '1997-07-01' + interval '3' month
					and exists (
						select
							*
						from
							lineitem
						where
							l_orderkey = o_orderkey
							and l_commitdate < l_receiptdate
					)
				group by
					o_orderpriority
				order by
					o_orderpriority
				;
				`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build15_q6(t *testing.T) {
	convey.Convey("tpch-q6", t, func() {
		sql := `select
					sum(l_extendedprice * l_discount) as revenue
				from
					lineitem
				where
					l_shipdate >= date '1994-01-01'
					and l_shipdate < date '1994-01-01' + interval '1' year
					and l_discount between 0.03 - 0.01 and 0.03 + 0.01
					and l_quantity < 24;
				`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build16_q7(t *testing.T) {
	convey.Convey("tpch-q7", t, func() {
		sql := `select
					supp_nation,
					cust_nation,
					l_year,
					sum(volume) as revenue
				from
					(
						select
							n1.n_name as supp_nation,
							n2.n_name as cust_nation,
							extract(year from l_shipdate) as l_year,
							l_extendedprice * (1 - l_discount) as volume
						from
							supplier,
							lineitem,
							orders,
							customer,
							nation n1,
							nation n2
						where
							s_suppkey = l_suppkey
							and o_orderkey = l_orderkey
							and c_custkey = o_custkey
							and s_nationkey = n1.n_nationkey
							and c_nationkey = n2.n_nationkey
							and (
								(n1.n_name = 'FRANCE' and n2.n_name = 'ARGENTINA')
								or (n1.n_name = 'ARGENTINA' and n2.n_name = 'FRANCE')
							)
							and l_shipdate between date '1995-01-01' and date '1996-12-31'
					) as shipping
				group by
					supp_nation,
					cust_nation,
					l_year
				order by
					supp_nation,
					cust_nation,
					l_year
				;

				`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build16_q8(t *testing.T) {
	convey.Convey("tpch-q8", t, func() {
		sql := `select
					o_year,
					(sum(case
						when nation = 'ARGENTINA' then volume
						else 0
					end) / sum(volume)) as mkt_share
				from
					(
						select
							extract(year from o_orderdate) as o_year,
							l_extendedprice * (1 - l_discount) as volume,
							n2.n_name as nation
						from
							part,
							supplier,
							lineitem,
							orders,
							customer,
							nation n1,
							nation n2,
							region
						where
							p_partkey = l_partkey
							and s_suppkey = l_suppkey
							and l_orderkey = o_orderkey
							and o_custkey = c_custkey
							and c_nationkey = n1.n_nationkey
							and n1.n_regionkey = r_regionkey
							and r_name = 'AMERICA'
							and s_nationkey = n2.n_nationkey
							and o_orderdate between date '1995-01-01' and date '1996-12-31'
							and p_type = 'ECONOMY BURNISHED TIN'
					) as all_nations
				group by
					o_year
				order by
					o_year
				;
				`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build16_q9(t *testing.T) {
	convey.Convey("tpch-q9", t, func() {
		sql := `select
					nation,
					o_year,
					sum(amount) as sum_profit
				from
					(
						select
							n_name as nation,
							extract(year from o_orderdate) as o_year,
							l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
						from
							part,
							supplier,
							lineitem,
							partsupp,
							orders,
							nation
						where
							s_suppkey = l_suppkey
							and ps_suppkey = l_suppkey
							and ps_partkey = l_partkey
							and p_partkey = l_partkey
							and o_orderkey = l_orderkey
							and s_nationkey = n_nationkey
							and p_name like '%pink%'
					) as profit
				group by
					nation,
					o_year
				order by
					nation,
					o_year desc
				;
				`
		ret, err := runCase(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func Test_build16_q11(t *testing.T) {
	convey.Convey("tpch-q11", t, func() {
		sql := `select
					ps_partkey,
					sum(ps_supplycost * ps_availqty) as value
				from
					partsupp,
					supplier,
					nation
				where
					ps_suppkey = s_suppkey
					and s_nationkey = n_nationkey
					and n_name = 'JAPAN'
				group by
					ps_partkey 
				having sum(ps_supplycost * ps_availqty) > (
							select
								sum(ps_supplycost * ps_availqty) * 0.0001000000
							from
								partsupp,
								supplier,
								nation
							where
								ps_suppkey = s_suppkey
								and s_nationkey = n_nationkey
								and n_name = 'JAPAN'
						)
				order by
					value desc
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
				n_name,
				sum(l_extendedprice * (1 - l_discount)) as revenue
			from
				customer,
				orders,
				lineitem,
				supplier,
				nation,
				region
			where
				c_custkey = o_custkey
				and l_orderkey = o_orderkey
				and l_suppkey = s_suppkey
				and c_nationkey = s_nationkey
				and s_nationkey = n_nationkey
				and n_regionkey = r_regionkey
				and r_name = 'AMERICA'
				and o_orderdate >= date '1994-01-01'
				and o_orderdate < date '1994-01-01' + interval '1' year
			group by
				n_name
			order by
				revenue desc
			;
			`
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
	fmt.Println("oldplan3", plan3.String())
	err = os.WriteFile("oldplan3.json", getJSON(plan3), 0777)
	if err != nil {
		return
	}
}
