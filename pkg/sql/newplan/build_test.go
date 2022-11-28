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
	var one tree.Statement
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
	plan2, err = sqlplan.BuildPlan(cc, one)
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
