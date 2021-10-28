package common

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestFilenames(t *testing.T) {
	workDir := "/work"
	spillDir := MakeSpillDir(workDir)
	metaDir := MakeMetaDir(workDir)
	dataDir := MakeDataDir(workDir)
	assert.Equal(t, "/work/spill", spillDir)
	assert.Equal(t, "/work/meta", metaDir)
	assert.Equal(t, "/work/data", dataDir)

	lock := MakeLockFileName(workDir, "work")
	assert.Equal(t, "/work/work.lock", lock)
	blk1 := MakeBlockFileName(workDir, "blk-1", 0, false)
	assert.Equal(t, "/work/data/blk-1.blk", blk1)
	blk2 := MakeBlockFileName(workDir, "blk-2", 0, true)
	assert.Equal(t, "/work/data/blk-2.blk.tmp", blk2)
	assert.True(t, IsTempFile(blk2))
	res, err := FilenameFromTmpfile(blk2)
	assert.Nil(t, err)
	assert.Equal(t, res, "/work/data/blk-2.blk")
	_, err = FilenameFromTmpfile(blk1)
	assert.NotNil(t, err)
	seg1 := MakeSegmentFileName(workDir, "seg-1", 0, false)
	assert.Equal(t, "/work/data/seg-1.seg", seg1)
	tblk1 := MakeTBlockFileName(workDir, "tblk-1", false)
	assert.Equal(t, "/work/data/tblk-1.tblk", tblk1)

	ckp1 := MakeInfoCkpFileName(workDir, "1", false)
	assert.Equal(t, "/work/meta/1.ckp", ckp1)
	tckp1 := MakeTableCkpFileName(workDir, "1", 0, false)
	assert.Equal(t, "/work/meta/1.tckp", tckp1)

	res, ok := ParseSegmentfileName(seg1)
	assert.True(t, ok)
	res, ok = ParseSegmentfileName(res)
	assert.False(t, ok)
	res, ok = ParseBlockfileName(blk1)
	assert.True(t, ok)
	res, ok = ParseBlockfileName(res)
	assert.False(t, ok)
	res, ok = ParseTBlockfileName(tblk1)
	assert.True(t, ok)
	res, ok = ParseTBlockfileName(res)
	assert.False(t, ok)
	_, ok = ParseInfoMetaName(strings.TrimPrefix(ckp1, "/work/meta/"))
	assert.True(t, ok)
	_, ok = ParseInfoMetaName("xxx.ckpp")
	assert.False(t, ok)
	assert.Panics(t, func() {
		_, ok = ParseInfoMetaName("xxx.ckp")
	})
	_, ok = ParseTableMetaName(strings.TrimPrefix(tckp1, "/work/meta/"))
	assert.True(t, ok)
	_, ok = ParseTableMetaName("xxx.tckpp")
	assert.False(t, ok)

	n := MakeFilename(workDir, FTTransientNode, "node", false)
	assert.Equal(t, "/work/spill/node.nod", n)

	assert.Panics(t, func() {
		MakeFilename("", 10, "", false)
	})
}
