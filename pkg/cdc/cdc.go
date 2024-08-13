// Copyright 2022 Matrix Origin
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

package cdc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

var sssid = 0

func RunDecoder(
	ctx context.Context,
	inQueue disttae.Queue[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]],
	outQueue disttae.Queue[tools.Pair[*disttae.TableCtx, *DecoderOutput]],
	codec Decoder,
	ar *ActiveRoutine,
) {
	for {
		select {
		case <-ar.Cancel:
			// TODO: do something
			return
		case <-ar.Pause:
			select {
			case <-ctx.Done():
				return
			case <-ar.Cancel:
				// TODO: do something
				return
			case <-ar.Resume:
			}
		default:
			//TODO: refine
			if inQueue.Size() != 0 {
				head := inQueue.Front()
				inQueue.Pop()
				fmt.Fprintln(os.Stderr, "^^^^^", "get ps of",
					head.Key.Db(), head.Key.Table(), head.Key.DBId(), head.Key.TableId())
				res := codec.Decode(ctx, head.Key, head.Value)
				outQueue.Push(tools.NewPair[*disttae.TableCtx, *DecoderOutput](head.Key, res))
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

func RunSinker(
	ctx context.Context,
	inQueue disttae.Queue[tools.Pair[*disttae.TableCtx, *DecoderOutput]],
	sinker Sinker,
	ar *ActiveRoutine,
) {
	for {
		select {
		case <-ar.Cancel:
			// TODO: do something
			return
		case <-ar.Pause:
			select {
			case <-ctx.Done():
				return
			case <-ar.Cancel:
				// TODO: do something
				return
			case <-ar.Resume:
			}
		default:
			if inQueue.Size() != 0 {
				head := inQueue.Front()
				inQueue.Pop()
				fmt.Fprintln(os.Stderr, "^^^^^", "get decoded sqls of",
					head.Key.Db(), head.Key.Table(), head.Key.DBId(), head.Key.TableId())
				err := sinker.Sink(ctx, head.Key, head.Value)
				if err != nil {
					fmt.Fprintln(os.Stderr, "consoleSinker.Sink error", err)
				}
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}
