package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func Test_build(t *testing.T) {
	convey.Convey("t", t, func() {
		sql := `select l_returnflag from lineitem;`
		one, err := parsers.ParseOne(dialect.MYSQL, sql)
		convey.So(err, convey.ShouldBeNil)

		_, err = BuildPlan(plan.NewMockCompilerContext(), one)
		convey.So(err, convey.ShouldBeNil)
	})
}
