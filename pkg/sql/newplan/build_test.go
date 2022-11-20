package newplan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/smartystreets/goconvey/convey"
)

func Test_build(t *testing.T) {
	var plan1, plan2 *plan.Plan
	convey.Convey("t", t, func() {
		sql := `select l_returnflag from lineitem;`
		one, err := parsers.ParseOne(dialect.MYSQL, sql)
		convey.So(err, convey.ShouldBeNil)
		cc := sqlplan.NewMockCompilerContext()
		plan1, err = BuildPlan(cc, one)
		fmt.Println(plan1.String())
		convey.So(err, convey.ShouldBeNil)
		plan2, err = sqlplan.BuildPlan(cc, one)
		fmt.Println()
		fmt.Println(plan2.String())
		convey.So(plan1.String() == plan2.String(), convey.ShouldBeTrue)

	})
}
