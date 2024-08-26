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
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func NewSinker(
	sinkConn *sql.DB,
	sinkUri string,
	inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput],
	tableId uint64,
	watermarkUpdater *WatermarkUpdater,
) (Sinker, error) {
	//TODO: remove console
	if strings.HasPrefix(strings.ToLower(sinkUri), "console://") {
		return NewConsoleSinker(inputCh), nil
	}
	sink, err := NewMysqlSink(sinkConn)
	if err != nil {
		return nil, err
	}

	return NewMysqlSinker(sink, inputCh, tableId, watermarkUpdater), nil
}

var _ Sinker = new(consoleSinker)

type consoleSinker struct {
	inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput]
}

func NewConsoleSinker(inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput]) Sinker {
	return &consoleSinker{
		inputCh: inputCh,
	}
}

func (s *consoleSinker) Sink(_ context.Context, data *DecoderOutput) error {
	fmt.Fprintln(os.Stderr, "====console sinker====")
	//fmt.Fprintln(os.Stderr, cdcCtx.Db(), cdcCtx.DBId(), cdcCtx.Table(), cdcCtx.TableId(), data.ts)
	if value, ok := data.sqlOfRows.Load().([][]byte); !ok {
		fmt.Fprintln(os.Stderr, "no sqlOfrows")
	} else {
		fmt.Fprintln(os.Stderr, "total rows sql", len(value))
		for i, sqlBytes := range value {
			plen := min(len(sqlBytes), 200)
			fmt.Fprintln(os.Stderr, i, string(sqlBytes[:plen]))
		}
	}

	if value, ok := data.sqlOfObjects.Load().([][]byte); !ok {
		fmt.Fprintln(os.Stderr, "no sqlOfObjects")
	} else {
		fmt.Fprintln(os.Stderr, "total objects sql", len(value))
		for i, sqlBytes := range value {
			plen := min(len(sqlBytes), 200)
			fmt.Fprintln(os.Stderr, i, string(sqlBytes[:plen]))
		}
	}

	if value, ok := data.sqlOfDeletes.Load().([][]byte); !ok {
		fmt.Fprintln(os.Stderr, "no sqlOfDeltas")
	} else {
		fmt.Fprintln(os.Stderr, "total deltas sql", len(value))
		for i, sqlBytes := range value {
			plen := min(len(sqlBytes), 200)
			fmt.Fprintln(os.Stderr, i, string(sqlBytes[:plen]))
		}
	}

	return nil
}

func (s *consoleSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	for {
		select {
		case <-ar.Pause:
			return

		case <-ar.Cancel:
			return

		case entry := <-s.inputCh:
			tableCtx := entry.Key
			decodeOutput := entry.Value
			_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)]\n",
				decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

			err := s.Sink(ctx, decodeOutput)
			if err != nil {
				return
			}
		}
	}
}

var _ Sinker = new(mysqlSinker)

type mysqlSinker struct {
	mysql            Sink
	inputCh          chan tools.Pair[*disttae.TableCtx, *DecoderOutput]
	tableId          uint64
	watermarkUpdater *WatermarkUpdater
}

func NewMysqlSinker(
	mysql Sink,
	inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput],
	tableId uint64,
	watermarkUpdater *WatermarkUpdater,
) Sinker {
	return &mysqlSinker{
		mysql:            mysql,
		inputCh:          inputCh,
		tableId:          tableId,
		watermarkUpdater: watermarkUpdater,
	}
}

func (s *mysqlSinker) Sink(ctx context.Context, data *DecoderOutput) error {
	return s.mysql.Send(ctx, data)
}

func (s *mysqlSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: start\n")
	defer func() {
		s.mysql.Close()
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: end\n")
	}()

	for {
		select {
		case <-ar.Pause:
			return

		case <-ar.Cancel:
			return

		case entry := <-s.inputCh:
			tableCtx := entry.Key
			decodeOutput := entry.Value

			// TODO use the watermark to filter the data
			//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s}\n", decodeOutput.ts.DebugString())
			watermark := s.watermarkUpdater.GetTableWatermark(s.tableId)
			if watermark.GreaterEq(decodeOutput.ts) {
				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: %s(%d) Unexpect watermark: %s, cur watermark: %s \n",
					tableCtx.Table(), tableCtx.TableId(), decodeOutput.ts.DebugString(), watermark.DebugString())
				continue
			}

			if s.tableId == HeartBeatTableId {
				s.watermarkUpdater.UpdateTableWatermark(s.tableId, decodeOutput.ts)
				continue
			}

			_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)]\n",
				decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

			if err := s.Sink(ctx, decodeOutput); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)], sink error: %v\n",
					decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId(),
					err,
				)
				// TODO handle error, stop cdc task
				continue
			}

			_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)], sink over\n",
				decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

			s.watermarkUpdater.UpdateTableWatermark(s.tableId, decodeOutput.ts)
		}
	}
}

type mysqlSink struct {
	conn *sql.DB
}

func NewMysqlSink(sinkConn *sql.DB) (Sink, error) {
	ret := &mysqlSink{
		conn: sinkConn,
	}
	return ret, nil
}

//func (s *mysqlSink) connect() (err error) {
//	s.conn, err = openDbConn(s.user, s.password, s.ip, s.port)
//	if err != nil {
//		return err
//	}
//	return err
//}

func (s *mysqlSink) Send(ctx context.Context, data *DecoderOutput) (err error) {
	sendRows := func(info string, rows [][]byte) (serr error) {
		fmt.Fprintln(os.Stderr, "----mysql sink----", info, len(rows))
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			plen := min(len(row), 200)
			fmt.Fprintln(os.Stderr, "----mysql sink----", info, string(row[:plen]))
			_, serr = s.conn.ExecContext(ctx, util.UnsafeBytesToString(row))
			if serr != nil {
				return serr
			}
		}
		return
	}
	fmt.Fprintln(os.Stderr, "----mysql sink begin----")
	defer fmt.Fprintln(os.Stderr, "----mysql sink end----")
	sqlOfRows := data.sqlOfRows.Load().([][]byte)
	err = sendRows("rows", sqlOfRows)
	if err != nil {
		return err
	}

	sqlOfObjects := data.sqlOfObjects.Load().([][]byte)
	err = sendRows("objects", sqlOfObjects)
	if err != nil {
		return err
	}

	sqlOfDeletes := data.sqlOfDeletes.Load().([][]byte)
	err = sendRows("deletes", sqlOfDeletes)
	if err != nil {
		return err
	}
	return
}

func (s *mysqlSink) Close() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

type matrixoneSink struct {
}

func (*matrixoneSink) Send(ctx context.Context, data *DecoderOutput) error {
	return nil
}

func extractUriInfo(ctx context.Context, uri string) (user string, pwd string, ip string, port int, err error) {
	slashIdx := strings.Index(uri, "//")
	if slashIdx == -1 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 1")
	}
	atIdx := strings.Index(uri[slashIdx+2:], "@")
	if atIdx == -1 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 2")
	}
	userPwd := uri[slashIdx+2:][:atIdx]
	seps := strings.Split(userPwd, ":")
	if len(seps) != 2 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 3")
	}
	user = seps[0]
	pwd = seps[1]
	ipPort := uri[slashIdx+2:][atIdx+1:]
	seps = strings.Split(ipPort, ":")
	if len(seps) != 2 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 4")
	}
	ip = seps[0]
	portStr := seps[1]
	var portInt int64
	portInt, err = strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 5 %v", portStr)
	}
	if portInt < 0 || portInt > 65535 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 6")
	}
	port = int(portInt)
	return
}