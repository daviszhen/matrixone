package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestReverse(t *testing.T) {
	convey.Convey("right", t, func() {
		inputStrs := []string{
			"abc",
			"abcd",
			"hello",
			"ｱｲｳｴｵ",
			"あいうえお",
			"龔龖龗龞龡",
			"你好",
			"再 见",
			"bcd",
			"def",
			"xyz",
			"1a1",
			"2012",
			"@($)@($#)_@(#",
			"2023-04-24",
			"10:03:23.021412",
		}
		wantStrs := []string{
			"cba",
			"dcba",
			"olleh",
			"ｵｴｳｲｱ",
			"おえういあ",
			"龡龞龗龖龔",
			"好你",
			"见 再",
			"dcb",
			"fed",
			"zyx",
			"1a1",
			"2102",
			"#(@_)#$(@)$(@",
			"42-40-3202",
			"214120.32:30:01",
		}
		ivec := testutil.MakeVarcharVector(inputStrs, nil)
		wantVec := testutil.MakeVarcharVector(wantStrs, nil)
		proc := testutil.NewProc()
		get, err := Reverse([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, get)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("null", t, func() {
		ivec := testutil.MakeScalarNull(10)
		wantvec := testutil.MakeScalarNull(10)
		proc := testutil.NewProc()
		ovec, err := Reverse([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, ovec)
		convey.So(ret, convey.ShouldBeTrue)

	})
	convey.Convey("tinyint", t, func() {
		ivec := testutil.MakeInt8Vector([]int8{
			1, 71, 1, 1}, nil)
		proc := testutil.NewProc()
		_, err := Reverse([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldNotBeNil)

		ivec2 := testutil.MakeScalarInt8(1, 10)
		_, err = Reverse([]*vector.Vector{ivec2}, proc)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("nil", t, func() {
		proc := testutil.NewProc()
		_, err := Reverse([]*vector.Vector{}, proc)
		convey.So(err, convey.ShouldNotBeNil)

		_, err = Reverse([]*vector.Vector{}, nil)
		convey.So(err, convey.ShouldNotBeNil)

		_, err = Reverse([]*vector.Vector{nil}, proc)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
