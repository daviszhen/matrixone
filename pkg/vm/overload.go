// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vm

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var stringFunc = [...]func(any, *bytes.Buffer){
	Top:        top.String,
	Join:       join.String,
	Semi:       semi.String,
	Left:       left.String,
	Single:     single.String,
	Limit:      limit.String,
	Order:      order.String,
	Group:      group.String,
	Merge:      merge.String,
	Output:     output.String,
	Offset:     offset.String,
	Product:    product.String,
	Restrict:   restrict.String,
	Dispatch:   dispatch.String,
	Connector:  connector.String,
	Projection: projection.String,
	Anti:       anti.String,

	LoopJoin:   loopjoin.String,
	LoopLeft:   loopleft.String,
	LoopSingle: loopsingle.String,
	LoopSemi:   loopsemi.String,
	LoopAnti:   loopanti.String,

	MergeTop:    mergetop.String,
	MergeLimit:  mergelimit.String,
	MergeOrder:  mergeorder.String,
	MergeGroup:  mergegroup.String,
	MergeOffset: mergeoffset.String,

	Deletion: deletion.String,
	Insert:   insert.String,
	Update:   update.String,

	Minus:     minus.String,
	Intersect: intersect.String,

	HashBuild: hashbuild.String,
}

var prepareFunc = [...]func(*process.Process, any) error{
	Top:        top.Prepare,
	Join:       join.Prepare,
	Semi:       semi.Prepare,
	Left:       left.Prepare,
	Single:     single.Prepare,
	Limit:      limit.Prepare,
	Order:      order.Prepare,
	Group:      group.Prepare,
	Merge:      merge.Prepare,
	Output:     output.Prepare,
	Offset:     offset.Prepare,
	Product:    product.Prepare,
	Restrict:   restrict.Prepare,
	Dispatch:   dispatch.Prepare,
	Connector:  connector.Prepare,
	Projection: projection.Prepare,
	Anti:       anti.Prepare,

	LoopJoin:   loopjoin.Prepare,
	LoopLeft:   loopleft.Prepare,
	LoopSingle: loopsingle.Prepare,
	LoopSemi:   loopsemi.Prepare,
	LoopAnti:   loopanti.Prepare,

	MergeTop:    mergetop.Prepare,
	MergeLimit:  mergelimit.Prepare,
	MergeOrder:  mergeorder.Prepare,
	MergeGroup:  mergegroup.Prepare,
	MergeOffset: mergeoffset.Prepare,

	Deletion: deletion.Prepare,
	Insert:   insert.Prepare,
	Update:   update.Prepare,

	Minus:     minus.Prepare,
	Intersect: intersect.Prepare,

	HashBuild: hashbuild.Prepare,
}

var execFunc = [...]func(int, *process.Process, any) (bool, error){
	Top:        top.Call,
	Join:       join.Call,
	Semi:       semi.Call,
	Left:       left.Call,
	Single:     single.Call,
	Limit:      limit.Call,
	Order:      order.Call,
	Group:      group.Call,
	Merge:      merge.Call,
	Output:     output.Call,
	Offset:     offset.Call,
	Product:    product.Call,
	Restrict:   restrict.Call,
	Dispatch:   dispatch.Call,
	Connector:  connector.Call,
	Projection: projection.Call,
	Anti:       anti.Call,

	LoopJoin:   loopjoin.Call,
	LoopLeft:   loopleft.Call,
	LoopSingle: loopsingle.Call,
	LoopSemi:   loopsemi.Call,
	LoopAnti:   loopanti.Call,

	MergeTop:    mergetop.Call,
	MergeLimit:  mergelimit.Call,
	MergeOrder:  mergeorder.Call,
	MergeGroup:  mergegroup.Call,
	MergeOffset: mergeoffset.Call,

	Deletion: deletion.Call,
	Insert:   insert.Call,
	Update:   update.Call,

	Minus:     minus.Call,
	Intersect: intersect.Call,

	HashBuild: hashbuild.Call,
}
