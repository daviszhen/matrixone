// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ExportConfig struct {
	// configs from user input
	UserConfig *tree.ExportParam

	// curFileSize
	CurFileSize uint64
	Rows        uint64
	FileCnt     uint
	ColumnFlag  []bool   // column : force quote or not
	Symbol      [][]byte // field separator of line initialized before export
	// default flush size
	DefaultBufSize int64
	OutputStr      []byte
	LineSize       uint64

	writtenCsvBytes uint64

	//file service & buffer for the line
	UseFileService bool

	diskFileConfig
	fsConfig
}

func (ec *ExportConfig) needExportToFile() bool {
	return ec != nil && ec.UserConfig != nil && ec.UserConfig.Outfile
}

func (ec *ExportConfig) sendBatch(bat *batch.Batch) {
	if bat != nil {
		ec.batchChan <- bat
	}
}

// fsConfig writes into fileservice
type fsConfig struct {
	FileService fileservice.FileService
	LineBuffer  *bytes.Buffer
	Ctx         context.Context
	AsyncReader *io.PipeReader
	AsyncWriter *io.PipeWriter
	AsyncGroup  *errgroup.Group
}

// diskFileConfig writes into disk file
type diskFileConfig struct {
	first bool

	// file handler
	File *os.File

	// bufio.writer
	Writer *bufio.Writer

	// DiskFile can be seeked and truncated.
	toDiskFile bool

	batchChan  chan *batch.Batch
	asyncGroup *errgroup.Group
}

var OpenFile = os.OpenFile
var escape byte = '"'

func initExportConfig(ctx context.Context, ec *ExportConfig, mrs *MysqlResultSet, bufSize int64) error {
	var err error
	//1. basic init
	ec.DefaultBufSize = bufSize
	ec.DefaultBufSize *= 1024 * 1024
	n := (int)(mrs.GetColumnCount())
	if n <= 0 {
		return moerr.NewInternalError(ctx, "the column count is zero")
	}
	// field separator
	ec.Symbol = make([][]byte, n)
	for i := 0; i < n-1; i++ {
		ec.Symbol[i] = []byte(ec.UserConfig.Fields.Terminated)
	}
	//line terminated
	ec.Symbol[n-1] = []byte(ec.UserConfig.Lines.TerminatedBy)
	//force quote column flag
	ec.ColumnFlag = make([]bool, len(mrs.Name2Index))
	for i := 0; i < len(ec.UserConfig.ForceQuote); i++ {
		col, ok := mrs.Name2Index[ec.UserConfig.ForceQuote[i]]
		if ok {
			ec.ColumnFlag[col] = true
		}
	}

	//2. init first file
	if err = openNewFile(ctx, ec, mrs); err != nil {
		return err
	}
	return err
}

var openNewFile = func(ctx context.Context, ep *ExportConfig, mrs *MysqlResultSet) error {
	lineSize := ep.LineSize
	var err error
	ep.CurFileSize = 0
	if !ep.UseFileService {
		filePath := getExportFilePath(ep.UserConfig.FilePath, ep.FileCnt)
		ep.File, err = OpenFile(filePath, os.O_RDWR|os.O_EXCL|os.O_CREATE, 0o666)
		if err != nil {
			return err
		}
		ep.Writer = bufio.NewWriterSize(ep.File, int(ep.DefaultBufSize))
	} else {
		//default 1MB
		if ep.LineBuffer == nil {
			ep.LineBuffer = &bytes.Buffer{}
		} else {
			ep.LineBuffer.Reset()
		}
		ep.AsyncReader, ep.AsyncWriter = io.Pipe()
		filePath := getExportFilePath(ep.UserConfig.FilePath, ep.FileCnt)

		asyncWriteFunc := func() error {
			vec := fileservice.IOVector{
				FilePath: filePath,
				Entries: []fileservice.IOEntry{
					{
						ReaderForWrite: ep.AsyncReader,
						Size:           -1,
					},
				},
			}
			err := ep.FileService.Write(ctx, vec)
			if err != nil {
				err2 := ep.AsyncReader.CloseWithError(err)
				if err2 != nil {
					return err2
				}
			}
			return err
		}

		ep.AsyncGroup = new(errgroup.Group)
		ep.AsyncGroup.Go(asyncWriteFunc)
	}
	if ep.UserConfig.Header {
		var header string
		n := len(mrs.Columns)
		if n == 0 {
			return nil
		}
		for i := 0; i < n-1; i++ {
			header += mrs.Columns[i].Name() + ep.UserConfig.Fields.Terminated
		}
		header += mrs.Columns[n-1].Name() + ep.UserConfig.Lines.TerminatedBy
		if ep.UserConfig.MaxFileSize != 0 && uint64(len(header)) >= ep.UserConfig.MaxFileSize {
			return moerr.NewInternalError(ctx, "the header line size is over the maxFileSize")
		}
		if err := writeFile(ep, []byte(header)); err != nil {
			return err
		}
		if _, err := EndOfLine(ep); err != nil {
			return err
		}
	}
	if lineSize != 0 {
		ep.LineSize = 0
		ep.Rows = 0
		if err := writeFile(ep, ep.OutputStr); err != nil {
			return err
		}
	}
	return nil
}

func getExportFilePath(filename string, fileCnt uint) string {
	if fileCnt == 0 {
		return filename
	} else {
		return fmt.Sprintf("%s.%d", filename, fileCnt)
	}
}

var formatOutputString = func(oq *outputQueue, tmp, symbol []byte, enclosed byte, flag bool) error {
	var err error
	if flag {
		if err = writeBytesToFile(oq, []byte{enclosed}); err != nil {
			return err
		}
	}
	if err = writeBytesToFile(oq, tmp); err != nil {
		return err
	}
	if flag {
		if err = writeBytesToFile(oq, []byte{enclosed}); err != nil {
			return err
		}
	}
	if err = writeBytesToFile(oq, symbol); err != nil {
		return err
	}
	return nil
}

var Flush = func(ep *ExportConfig) error {
	if !ep.UseFileService {
		return ep.Writer.Flush()
	}
	return nil
}

var Seek = func(ep *ExportConfig) (int64, error) {
	if !ep.UseFileService {
		return ep.File.Seek(int64(ep.CurFileSize-ep.LineSize), io.SeekStart)
	}
	return 0, nil
}

var Read = func(ep *ExportConfig) (int, error) {
	if !ep.UseFileService {
		ep.OutputStr = make([]byte, ep.LineSize)
		return ep.File.Read(ep.OutputStr)
	} else {
		ep.OutputStr = make([]byte, ep.LineSize)
		copy(ep.OutputStr, ep.LineBuffer.Bytes())
		ep.LineBuffer.Reset()
		return int(ep.LineSize), nil
	}
}

var Truncate = func(ep *ExportConfig) error {
	if !ep.UseFileService {
		return ep.File.Truncate(int64(ep.CurFileSize - ep.LineSize))
	} else {
		return nil
	}
}

var Close = func(ep *ExportConfig) error {
	if !ep.UseFileService {
		ep.FileCnt++
		return ep.File.Close()
	} else {
		ep.FileCnt++
		err := ep.AsyncWriter.Close()
		if err != nil {
			return err
		}
		err = ep.AsyncGroup.Wait()
		if err != nil {
			return err
		}
		err = ep.AsyncReader.Close()
		if err != nil {
			return err
		}
		ep.AsyncReader = nil
		ep.AsyncWriter = nil
		ep.AsyncGroup = nil
		return err
	}
}

var Write = func(ep *ExportConfig, output []byte) (int, error) {
	if !ep.UseFileService {
		return ep.Writer.Write(output)
	} else {
		return ep.LineBuffer.Write(output)
	}
}

var EndOfLine = func(ep *ExportConfig) (int, error) {
	if ep.UseFileService {
		n, err := ep.AsyncWriter.Write(ep.LineBuffer.Bytes())
		if err != nil {
			err2 := ep.AsyncWriter.CloseWithError(err)
			if err2 != nil {
				return 0, err2
			}
		}
		ep.LineBuffer.Reset()
		return n, err
	}
	return 0, nil
}

var writeFile = func(ep *ExportConfig, output []byte) error {
	for {
		if n, err := Write(ep, output); err != nil {
			return err
		} else if n == len(output) {
			break
		}
	}
	ep.LineSize += uint64(len(output))
	ep.CurFileSize += uint64(len(output))
	ep.writtenCsvBytes += uint64(len(output))
	return nil
}

// writeBytesToFile writes bytes into file.
// if the file size is over the maxFileSize, it will close the file and open a new file.
func writeBytesToFile(oq *outputQueue, output []byte) error {
	if oq.ep.UserConfig.MaxFileSize != 0 && oq.ep.CurFileSize+uint64(len(output)) > oq.ep.UserConfig.MaxFileSize {
		if err := Flush(oq.ep); err != nil {
			return err
		}
		if oq.ep.LineSize != 0 && oq.ep.toDiskFile {
			if _, err := Seek(oq.ep); err != nil {
				return err
			}
			for {
				if n, err := Read(oq.ep); err != nil {
					return err
				} else if uint64(n) == oq.ep.LineSize {
					break
				}
			}
			if err := Truncate(oq.ep); err != nil {
				return err
			}
		}
		if err := Close(oq.ep); err != nil {
			return err
		}
		if err := openNewFile(oq.ctx, oq.ep, oq.mrs); err != nil {
			return err
		}
	}

	if err := writeFile(oq.ep, output); err != nil {
		return err
	}
	return nil
}

func copyBatch(ses *Session, bat *batch.Batch) (*batch.Batch, error) {
	var err error
	bat2 := batch.NewWithSize(len(bat.Vecs))
	for i, vec := range bat.Vecs {
		bat2.Vecs[i], err = vec.Dup(ses.GetMemPool())
		if err != nil {
			return nil, err
		}
	}
	bat2.SetRowCount(bat.RowCount())
	return bat2, err
}

func initAsyncExport(ctx context.Context, ses *Session, ec *ExportConfig) {
	if !ec.first {
		ec.first = true
		ec.toDiskFile = true
		ec.batchChan = make(chan *batch.Batch)
		ec.asyncGroup = new(errgroup.Group)
		ec.asyncGroup.Go(func() error {
			return doExport(ctx, ses, ec)
		})
	}
}

func doExport(ctx context.Context, ses *Session, ec *ExportConfig) error {
	var err error
	var quit bool
	var bat *batch.Batch
	var ok bool
	for {
		select {
		case <-ctx.Done():
			quit = true
		case bat, ok = <-ec.batchChan:
			if !ok {
				quit = true
			}
		}
		if quit {
			break
		}

		if bat == nil {
			continue
		}

		oq := NewOutputQueue(ctx, ses, len(bat.Vecs), nil, ec)
		err = writeBatch(ctx, ses, oq, bat)
		if err != nil {
			return err
		}

		err = oq.flush()
		if err != nil {
			return err
		}
	}

	return err
}

func writeBatch(ctx context.Context, ses *Session, oq *outputQueue, bat *batch.Batch) error {
	var err error
	n := bat.Vecs[0].Length()
	defer func() {
		bat.Clean(ses.GetMemPool())
	}()
	for j := 0; j < n; j++ { //row index
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		//do not need copy bytes here. because the batch has been copied.
		_, err = extractRowFromEveryVector(ses, bat, j, oq, false)
		if err != nil {
			return err
		}
		//if oq.showStmtType == ShowTableStatus {
		//	row2 := make([]interface{}, len(row))
		//	copy(row2, row)
		//	ses.AppendData(row2)
		//}
	}
	return err
}

func addEscapeToString(s []byte) []byte {
	pos := make([]int, 0)
	for i := 0; i < len(s); i++ {
		if s[i] == escape {
			pos = append(pos, i)
		}
	}
	if len(pos) == 0 {
		return s
	}
	ret := make([]byte, 0)
	cur := 0
	for i := 0; i < len(pos); i++ {
		ret = append(ret, s[cur:pos[i]]...)
		ret = append(ret, escape)
		cur = pos[i]
	}
	ret = append(ret, s[cur:]...)
	return ret
}

func exportDataToCSVFile(oq *outputQueue) error {
	oq.ep.LineSize = 0

	symbol := oq.ep.Symbol
	closeby := oq.ep.UserConfig.Fields.EnclosedBy
	flag := oq.ep.ColumnFlag
	for i := uint64(0); i < oq.mrs.GetColumnCount(); i++ {
		column, err := oq.mrs.GetColumn(oq.ctx, i)
		if err != nil {
			return err
		}
		mysqlColumn, ok := column.(*MysqlColumn)
		if !ok {
			return moerr.NewInternalError(oq.ctx, "sendColumn need MysqlColumn")
		}
		if isNil, err := oq.mrs.ColumnIsNull(oq.ctx, 0, i); err != nil {
			return err
		} else if isNil {
			//NULL is output as \N
			if err = formatOutputString(oq, []byte{'\\', 'N'}, symbol[i], closeby, false); err != nil {
				return err
			}
			continue
		}

		switch mysqlColumn.ColumnType() {
		case defines.MYSQL_TYPE_DECIMAL:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_BOOL:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
			value, err := oq.mrs.GetInt64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
				if value == 0 {
					if err = formatOutputString(oq, []byte("0000"), symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				} else {
					oq.resetLineStr()
					oq.lineStr = strconv.AppendInt(oq.lineStr, value, 10)
					if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				}
			} else {
				oq.resetLineStr()
				oq.lineStr = strconv.AppendInt(oq.lineStr, value, 10)
				if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE:
			value, err := oq.mrs.GetFloat64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			oq.lineStr = []byte(fmt.Sprintf("%v", value))
			if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
				if value, err := oq.mrs.GetUint64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					oq.resetLineStr()
					oq.lineStr = strconv.AppendUint(oq.lineStr, value, 10)
					if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				}
			} else {
				if value, err := oq.mrs.GetInt64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					oq.resetLineStr()
					oq.lineStr = strconv.AppendInt(oq.lineStr, value, 10)
					if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				}
			}
		// Binary/varbinary has mysql_type_varchar.
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
			defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TEXT:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			value = addEscapeToString(value.([]byte))
			if err = formatOutputString(oq, value.([]byte), symbol[i], closeby, true); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATE:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Date).String()), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Time).String()), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATETIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(string)), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIMESTAMP:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_JSON:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			jsonStr := value.(bytejson.ByteJson).String()
			if err = formatOutputString(oq, []byte(jsonStr), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_UUID:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		default:
			return moerr.NewInternalError(oq.ctx, "unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}
	oq.ep.Rows++
	_, err := EndOfLine(oq.ep)
	return err
}

func finishExport(ctx context.Context, ses *Session, ec *ExportConfig) error {
	var err error
	if !ec.UseFileService {
		//close chan to notify the writer to quit
		close(ec.batchChan)
		err = ec.asyncGroup.Wait()
		defer func() {
			ec.asyncGroup = nil
			ec.batchChan = nil
		}()
		if err != nil {
			return err
		}

		if err = Flush(ec); err != nil {
			return err
		}
		if err = Close(ec); err != nil {
			return err
		}
		ses.writeCsvBytes.Add(int64(ec.writtenCsvBytes))
	} else {
		ec.LineBuffer = nil
		ec.OutputStr = nil
		if ec.AsyncReader != nil {
			_ = ec.AsyncReader.Close()
		}
		if ec.AsyncWriter != nil {
			_ = ec.AsyncWriter.Close()
		}
		ec.FileService = nil
		ec.Ctx = nil
	}
	return err
}
