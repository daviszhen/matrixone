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

package blockio

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type Writer struct {
	writer   objectio.Writer
	fs       *objectio.ObjectFS
	writeCxt context.Context
}

func NewWriter(ctx context.Context, fs *objectio.ObjectFS, name string) *Writer {
	writer, err := objectio.NewObjectWriter(name, fs.Service)
	if err != nil {
		panic(any(err))
	}
	return &Writer{
		fs:       fs,
		writer:   writer,
		writeCxt: ctx,
	}
}

func (w *Writer) WriteBlock(columns *containers.Batch) (block objectio.BlockObject, err error) {
	bat := batch.New(true, columns.Attrs)
	bat.Vecs = containers.UnmarshalToMoVecs(columns.Vecs)
	block, err = w.writer.Write(bat)
	return
}

func (w *Writer) WriteBlockAndZoneMap(batch *batch.Batch, idxs []uint16) (objectio.BlockObject, error) {
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}
	for _, idx := range idxs {
		var zoneMap objectio.IndexData
		vec := containers.NewVectorWithSharedMemory(batch.Vecs[idx], true)
		zm := index.NewZoneMap(batch.Vecs[idx].Typ)
		ctx := new(index.KeysCtx)
		ctx.Keys = vec
		ctx.Count = batch.Vecs[idx].Length()
		defer ctx.Keys.Close()
		err = zm.BatchUpdate(ctx)
		if err != nil {
			return nil, err
		}
		buf, err := zm.Marshal()
		if err != nil {
			return nil, err
		}
		zoneMap, err = objectio.NewZoneMap(idx, buf)
		if err != nil {
			return nil, err
		}
		w.writer.WriteIndex(block, zoneMap)
	}
	return block, nil
}

func (w *Writer) Sync() ([]objectio.BlockObject, error) {
	blocks, err := w.writer.WriteEnd(w.writeCxt)
	return blocks, err
}

func (w *Writer) WriteIndex(
	block objectio.BlockObject,
	index objectio.IndexData) (err error) {
	return w.writer.WriteIndex(block, index)
}

func (w *Writer) GetWriter() objectio.Writer {
	return w.writer
}
