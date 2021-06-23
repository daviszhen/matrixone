package index

import (
	"fmt"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
	"sync/atomic"
)

type PostCloseCB = func(interface{})

type SegmentHolder struct {
	common.RefHelper
	ID     common.ID
	BufMgr mgrif.IBufferManager
	Inited bool
	self   struct {
		sync.RWMutex
		Indexes    []*Node
		ColIndexes map[int][]int
	}
	tree struct {
		sync.RWMutex
		Blocks   []*BlockHolder
		IdMap    map[uint64]int
		BlockCnt int32
	}
	Type        base.SegmentType
	PostCloseCB PostCloseCB
}

func newSegmentHolder(bufMgr mgrif.IBufferManager, id common.ID, segType base.SegmentType, cb PostCloseCB) *SegmentHolder {
	holder := &SegmentHolder{ID: id, Type: segType, BufMgr: bufMgr}
	holder.tree.Blocks = make([]*BlockHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	holder.self.ColIndexes = make(map[int][]int)
	holder.self.Indexes = make([]*Node, 0)
	holder.OnZeroCB = holder.close
	holder.PostCloseCB = cb
	holder.Ref()
	return holder
}

func (holder *SegmentHolder) Init(segFile base.ISegmentFile) {
	if holder.Inited {
		panic("logic error")
	}
	indexesMeta := segFile.GetIndexesMeta()
	if indexesMeta == nil {
		return
	}
	for _, meta := range indexesMeta.Data {
		vf := segFile.MakeVirtualIndexFile(meta)
		col := int(meta.Cols.Slice()[0])
		node := newNode(holder.BufMgr, vf, ZoneMapIndexConstructor, meta.Ptr.Len, meta.Cols, nil)
		idxes, ok := holder.self.ColIndexes[col]
		if !ok {
			idxes = make([]int, 0)
			holder.self.ColIndexes[col] = idxes
		}
		holder.self.ColIndexes[col] = append(holder.self.ColIndexes[col], len(holder.self.Indexes))
		holder.self.Indexes = append(holder.self.Indexes, node)
	}
	holder.Inited = true
}

func (holder *SegmentHolder) close() {
	for _, blk := range holder.tree.Blocks {
		blk.Unref()
	}

	for _, colIndex := range holder.self.Indexes {
		colIndex.Unref()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}

func (holder *SegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	idxes, ok := holder.self.ColIndexes[colIdx]
	if !ok {
		// TODO
		ctx.BoolRes = true
		return nil
	}
	var err error
	for _, idx := range idxes {
		node := holder.self.Indexes[idx].GetManagedNode()
		err = node.DataNode.(Index).Eval(ctx)
		if err != nil {
			node.Close()
			return err
		}
		node.Close()
	}
	return nil
}

func (holder *SegmentHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexSegmentHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.SegmentString(), holder.Type,
		holder.tree.BlockCnt, holder.RefCount())
	for _, blk := range holder.tree.Blocks {
		s = fmt.Sprintf("%s\n\t%s", s, blk.stringNoLock())
	}
	return s
}

func (holder *SegmentHolder) GetBlock(id uint64) (blk *BlockHolder) {
	holder.tree.RLock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		holder.tree.RUnlock()
		return nil
	}
	blk = holder.tree.Blocks[idx]
	blk.Ref()
	holder.tree.RUnlock()
	return blk
}

func (holder *SegmentHolder) RegisterBlock(id common.ID, blkType base.BlockType, cb PostCloseCB) *BlockHolder {
	blk := newBlockHolder(holder.BufMgr, id, blkType, cb)
	holder.addBlock(blk)
	blk.Ref()
	return blk
}

func (holder *SegmentHolder) addBlock(blk *BlockHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	_, ok := holder.tree.IdMap[blk.ID.BlockID]
	if ok {
		panic(fmt.Sprintf("Duplicate blk %s for seg %s", blk.ID.BlockString(), holder.ID.SegmentString()))
	}
	holder.tree.IdMap[blk.ID.BlockID] = len(holder.tree.Blocks)
	holder.tree.Blocks = append(holder.tree.Blocks, blk)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(1))
}

func (holder *SegmentHolder) DropBlock(id uint64) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("Specified blk %d not found in seg %d", id, holder.ID))
	}
	dropped := holder.tree.Blocks[idx]
	delete(holder.tree.IdMap, id)
	holder.tree.Blocks = append(holder.tree.Blocks[:idx], holder.tree.Blocks[idx+1:]...)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(-1))
	return dropped
}

func (holder *SegmentHolder) GetBlockCount() int32 {
	return atomic.LoadInt32(&holder.tree.BlockCnt)
}

func (holder *SegmentHolder) UpgradeBlock(id uint64, blkType base.BlockType) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("specified blk %d not found in %d", id, holder.ID))
	}
	stale := holder.tree.Blocks[idx]
	if stale.Type >= blkType {
		panic(fmt.Sprintf("Cannot upgrade blk %d, type %d", id, blkType))
	}
	blk := newBlockHolder(holder.BufMgr, stale.ID, blkType, stale.PostCloseCB)
	holder.tree.Blocks[idx] = blk
	blk.Ref()
	stale.Unref()
	return blk
}
