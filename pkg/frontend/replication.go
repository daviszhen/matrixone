package frontend

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/aws/smithy-go/rand"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/shopspring/decimal"
	"github.com/siddontang/go/hack"
	"hash/crc32"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unsafe"
)

// RowsEventStmtEndFlag is set in the end of the statement.
const RowsEventStmtEndFlag = 0x01
const TIMEF_OFS int64 = 0x800000000000
const TIMEF_INT_OFS int64 = 0x800000
const defaultBufferSize = 65536 // 64kb
const TooBigBlockSize = 1024 * 1024 * 4
const defaultAuthPluginName = AUTH_NATIVE_PASSWORD
const DEFAULT_COLLATION_NAME string = "utf8_general_ci"

// ref: https://github.com/mysql/mysql-server/blob/a9b0c712de3509d8d08d3ba385d41a4df6348775/strings/decimal.c#L137
const digitsPerInteger int = 9
const DATETIMEF_INT_OFS int64 = 0x8000000000

const (
	ClassicProtocolVersion byte   = 10
	XProtocolVersion       byte   = 11
	MaxPayloadLen          int    = 1<<24 - 1
	TimeFormat             string = "2006-01-02 15:04:05"
)

// On The Wire: Field Types
// See also binary_log::codecs::binary::Transaction_payload::fields in MySQL
// https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1codecs_1_1binary_1_1Transaction__payload.html#a9fff7ac12ba064f40e9216565c53d07b
const (
	OTW_PAYLOAD_HEADER_END_MARK = iota
	OTW_PAYLOAD_SIZE_FIELD
	OTW_PAYLOAD_COMPRESSION_TYPE_FIELD
	OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD
)

// Compression Types
const (
	ZSTD = 0
	NONE = 255
)

const (
	// Store JSON updates in partial form
	EnumBinlogRowValueOptionsPartialJsonUpdates = byte(iota + 1)
)

const (
	EnumRowImageTypeWriteAI = byte(iota)
	EnumRowImageTypeUpdateBI
	EnumRowImageTypeUpdateAI
	EnumRowImageTypeDeleteBI
)

const (
	ENUM_EXTRA_ROW_INFO_TYPECODE_NDB byte = iota
	ENUM_EXTRA_ROW_INFO_TYPECODE_PARTITION
)

const (
	EventHeaderSize            = 19
	SidLength                  = 16
	LogicalTimestampTypeCode   = 2
	PartLogicalTimestampLength = 8
	BinlogChecksumLength       = 4
	UndefinedServerVer         = 999999 // UNDEFINED_SERVER_VERSION
)

const (
	AUTH_MYSQL_OLD_PASSWORD    = "mysql_old_password"
	AUTH_NATIVE_PASSWORD       = "mysql_native_password"
	AUTH_CLEAR_PASSWORD        = "mysql_clear_password"
	AUTH_CACHING_SHA2_PASSWORD = "caching_sha2_password"
	AUTH_SHA256_PASSWORD       = "sha256_password"
)

const (
	OK_HEADER          byte = 0x00
	MORE_DATE_HEADER   byte = 0x01
	ERR_HEADER         byte = 0xff
	EOF_HEADER         byte = 0xfe
	LocalInFile_HEADER byte = 0xfb

	CACHE_SHA2_FAST_AUTH byte = 0x03
	CACHE_SHA2_FULL_AUTH byte = 0x04
)

const (
	jsonbSmallOffsetSize = 2
	jsonbLargeOffsetSize = 4

	jsonbKeyEntrySizeSmall = 2 + jsonbSmallOffsetSize
	jsonbKeyEntrySizeLarge = 2 + jsonbLargeOffsetSize

	jsonbValueEntrySizeSmall = 1 + jsonbSmallOffsetSize
	jsonbValueEntrySizeLarge = 1 + jsonbLargeOffsetSize
)

const (
	FieldValueTypeNull = iota
	FieldValueTypeUnsigned
	FieldValueTypeSigned
	FieldValueTypeFloat
	FieldValueTypeString
)

const (
	NOT_NULL_FLAG       = 1
	PRI_KEY_FLAG        = 2
	UNIQUE_KEY_FLAG     = 4
	BLOB_FLAG           = 16
	UNSIGNED_FLAG       = 32
	ZEROFILL_FLAG       = 64
	BINARY_FLAG         = 128
	ENUM_FLAG           = 256
	AUTO_INCREMENT_FLAG = 512
	TIMESTAMP_FLAG      = 1024
	SET_FLAG            = 2048
	NUM_FLAG            = 32768
	PART_KEY_FLAG       = 16384
	GROUP_FLAG          = 32768
	UNIQUE_FLAG         = 65536
)

const (
	JSONB_NULL_LITERAL  byte = 0x00
	JSONB_TRUE_LITERAL  byte = 0x01
	JSONB_FALSE_LITERAL byte = 0x02
)

const (
	MYSQL_TYPE_DECIMAL byte = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT

	// mysql 5.6
	MYSQL_TYPE_TIMESTAMP2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIME2
)

const (
	MYSQL_TYPE_JSON byte = iota + 0xf5
	MYSQL_TYPE_NEWDECIMAL
	MYSQL_TYPE_ENUM
	MYSQL_TYPE_SET
	MYSQL_TYPE_TINY_BLOB
	MYSQL_TYPE_MEDIUM_BLOB
	MYSQL_TYPE_LONG_BLOB
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING
	MYSQL_TYPE_GEOMETRY
)

const (
	JSONB_SMALL_OBJECT byte = iota // small JSON object
	JSONB_LARGE_OBJECT             // large JSON object
	JSONB_SMALL_ARRAY              // small JSON array
	JSONB_LARGE_ARRAY              // large JSON array
	JSONB_LITERAL                  // literal (true/false/null)
	JSONB_INT16                    // int16
	JSONB_UINT16                   // uint16
	JSONB_INT32                    // int32
	JSONB_UINT32                   // uint32
	JSONB_INT64                    // int64
	JSONB_UINT64                   // uint64
	JSONB_DOUBLE                   // double
	JSONB_STRING                   // string
	JSONB_OPAQUE       byte = 0x0f // custom data (any MySQL data type)
)

const (
	UNKNOWN_EVENT EventType = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	WRITE_ROWS_EVENTv0
	UPDATE_ROWS_EVENTv0
	DELETE_ROWS_EVENTv0
	WRITE_ROWS_EVENTv1
	UPDATE_ROWS_EVENTv1
	DELETE_ROWS_EVENTv1
	INCIDENT_EVENT
	HEARTBEAT_EVENT
	IGNORABLE_EVENT
	ROWS_QUERY_EVENT
	WRITE_ROWS_EVENTv2
	UPDATE_ROWS_EVENTv2
	DELETE_ROWS_EVENTv2
	GTID_EVENT
	ANONYMOUS_GTID_EVENT
	PREVIOUS_GTIDS_EVENT
	TRANSACTION_CONTEXT_EVENT
	VIEW_CHANGE_EVENT
	XA_PREPARE_LOG_EVENT
	PARTIAL_UPDATE_ROWS_EVENT
	TRANSACTION_PAYLOAD_EVENT
	HEARTBEAT_LOG_EVENT_V2
	GTID_TAGGED_LOG_EVENT
)

const (
	// The JSON value in the given path is replaced with a new value.
	//
	// It has the same effect as `JSON_REPLACE(col, path, value)`.
	JsonDiffOperationReplace = byte(iota)

	// Add a new element at the given path.
	//
	//  If the path specifies an array element, it has the same effect as `JSON_ARRAY_INSERT(col, path, value)`.
	//
	//  If the path specifies an object member, it has the same effect as `JSON_INSERT(col, path, value)`.
	JsonDiffOperationInsert

	// The JSON value at the given path is removed from an array or object.
	//
	// It has the same effect as `JSON_REMOVE(col, path)`.
	JsonDiffOperationRemove
)

const (
	BINLOG_CHECKSUM_ALG_OFF byte = 0 // Events are without checksum though its generator
	// is checksum-capable New Master (NM).
	BINLOG_CHECKSUM_ALG_CRC32 byte = 1 // CRC32 of zlib algorithm.
	//  BINLOG_CHECKSUM_ALG_ENUM_END,  // the cut line: valid alg range is [1, 0x7f].
	BINLOG_CHECKSUM_ALG_UNDEF byte = 255 // special value to tag undetermined yet checksum
	// or events from checksum-unaware servers
)

// These are TABLE_MAP_EVENT's optional metadata field type, from: libbinlogevents/include/rows_event.h
const (
	TABLE_MAP_OPT_META_SIGNEDNESS byte = iota + 1
	TABLE_MAP_OPT_META_DEFAULT_CHARSET
	TABLE_MAP_OPT_META_COLUMN_CHARSET
	TABLE_MAP_OPT_META_COLUMN_NAME
	TABLE_MAP_OPT_META_SET_STR_VALUE
	TABLE_MAP_OPT_META_ENUM_STR_VALUE
	TABLE_MAP_OPT_META_GEOMETRY_TYPE
	TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY
	TABLE_MAP_OPT_META_PRIMARY_KEY_WITH_PREFIX
	TABLE_MAP_OPT_META_ENUM_AND_SET_DEFAULT_CHARSET
	TABLE_MAP_OPT_META_ENUM_AND_SET_COLUMN_CHARSET
	TABLE_MAP_OPT_META_COLUMN_VISIBILITY
)

var zeros = [digitsPerInteger]byte{48, 48, 48, 48, 48, 48, 48, 48, 48}
var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
var gtidExp = regexp.MustCompile(`(\w{8}(-\w{4}){3}-\w{12}(:\d+(-\d+)?)+)`)
var (
	bytesBufferPool = sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}
)

var (
	byteSlicePool = sync.Pool{
		New: func() interface{} {
			return new(ByteSlice)
		},
	}
)

var (
	resultsetPool = sync.Pool{
		New: func() interface{} {
			return &Resultset{}
		},
	}
)

var (
	checksumVersionSplitMysql   = []int{5, 6, 1}
	checksumVersionProductMysql = (checksumVersionSplitMysql[0]*256+checksumVersionSplitMysql[1])*256 + checksumVersionSplitMysql[2]
)

// defines the supported auth plugins
var supportedAuthPlugins = []string{AUTH_NATIVE_PASSWORD, AUTH_SHA256_PASSWORD, AUTH_CACHING_SHA2_PASSWORD}

type IntervalSlice []Interval
type GTIDSet map[string]*UUIDSet
type EventType byte
type Option func(*RConn) error
type ByteSlice []byte
type RowData []byte

type RowsQueryEvent struct {
	Query []byte
}

type jsonBinaryDecoder struct {
	useDecimal      bool
	ignoreDecodeErr bool
	err             error
}

type PreviousGTIDsEvent struct {
	GTIDSets string
}

type JsonDiff struct {
	Op    byte
	Path  string
	Value string
}

type Stmt struct {
	conn *RConn
	id   uint32

	params   int
	columns  int
	warnings int
}

// fracTime is a help structure wrapping Golang Time.
type fracTime struct {
	time.Time

	// Dec must in [0, 6]
	Dec int

	timestampStringLocation *time.Location
}

// BinlogSyncerConfig is the configuration for BinlogSyncer.
type BinlogSyncerConfig struct {
	// ServerID is the unique ID in cluster.
	ServerID uint32
	// Host is for MySQL server host.
	Host string
	// Port is for MySQL server port.
	Port uint16
	// User is for MySQL user.
	User string
	// Password is for MySQL password.
	Password string

	EventCacheCount int

	DiscardGTIDSet bool
}

type EventError struct {
	Header *EventHeader

	//Error message
	Err string

	//Event data
	Data []byte
}

type RotateEvent struct {
	Position    uint64
	NextLogName []byte
}

type IntVarEvent struct {
	Type  byte
	Value uint64
}

type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	Schema        []byte
	Query         []byte

	// for mariadb QUERY_COMPRESSED_EVENT
	compressed bool

	// in fact QueryEvent dosen't have the GTIDSet information, just for beneficial to use
	GSet GTIDSet
}

type XIDEvent struct {
	XID uint64

	// in fact XIDEvent dosen't have the GTIDSet information, just for beneficial to use
	GSet GTIDSet
}

type GTIDEvent struct {
	CommitFlag     uint8
	SID            []byte
	GNO            int64
	LastCommitted  int64
	SequenceNumber int64

	// ImmediateCommitTimestamp/OriginalCommitTimestamp are introduced in MySQL-8.0.1, see:
	// https://mysqlhighavailability.com/replication-features-in-mysql-8-0-1/
	ImmediateCommitTimestamp uint64
	OriginalCommitTimestamp  uint64

	// Total transaction length (including this GTIDEvent), introduced in MySQL-8.0.2, see:
	// https://mysqlhighavailability.com/taking-advantage-of-new-transaction-length-metadata/
	TransactionLength uint64

	// ImmediateServerVersion/OriginalServerVersion are introduced in MySQL-8.0.14, see
	// https://dev.mysql.com/doc/refman/8.0/en/replication-compatibility.html
	ImmediateServerVersion uint32
	OriginalServerVersion  uint32
}

type FormatDescriptionEvent struct {
	Version                uint16
	ServerVersion          string
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte

	// 0 is off, 1 is for CRC32, 255 is undefined
	ChecksumAlgorithm byte
}

type TransactionPayloadEvent struct {
	format           FormatDescriptionEvent
	Size             uint64
	UncompressedSize uint64
	CompressionType  uint64
	Payload          []byte
	Events           []*BinlogEvent
}

type ExecuteLoadQueryEvent struct {
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVars       uint16
	FileID           uint32
	StartPos         uint32
	EndPos           uint32
	DupHandlingFlags uint8
}

type TableMapEvent struct {
	flavor      string
	tableIDSize int

	TableID uint64

	Flags uint16

	Schema []byte
	Table  []byte

	ColumnCount uint64
	ColumnType  []byte
	ColumnMeta  []uint16

	// len = (ColumnCount + 7) / 8
	NullBitmap []byte

	/*
		The following are available only after MySQL-8.0.1 or MariaDB-10.5.0
		By default MySQL and MariaDB do not log the full row metadata.
		see:
			- https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
			- https://mariadb.com/kb/en/replication-and-binary-log-system-variables/#binlog_row_metadata
	*/

	// SignednessBitmap stores signedness info for numeric columns.
	SignednessBitmap []byte

	// DefaultCharset/ColumnCharset stores collation info for character columns.

	// DefaultCharset[0] is the default collation of character columns.
	// For character columns that have different charset,
	// (character column index, column collation) pairs follows
	DefaultCharset []uint64
	// ColumnCharset contains collation sequence for all character columns
	ColumnCharset []uint64

	// SetStrValue stores values for set columns.
	SetStrValue       [][][]byte
	setStrValueString [][]string

	// EnumStrValue stores values for enum columns.
	EnumStrValue       [][][]byte
	enumStrValueString [][]string

	// ColumnName list all column names.
	ColumnName       [][]byte
	columnNameString []string // the same as ColumnName in string type, just for reuse

	// GeometryType stores real type for geometry columns.
	GeometryType []uint64

	// PrimaryKey is a sequence of column indexes of primary key.
	PrimaryKey []uint64

	// PrimaryKeyPrefix is the prefix length used for each column of primary key.
	// 0 means that the whole column length is used.
	PrimaryKeyPrefix []uint64

	// EnumSetDefaultCharset/EnumSetColumnCharset is similar to DefaultCharset/ColumnCharset but for enum/set columns.
	EnumSetDefaultCharset []uint64
	EnumSetColumnCharset  []uint64

	// VisibilityBitmap stores bits that are set if corresponding column is not invisible (MySQL 8.0.23+)
	VisibilityBitmap []byte

	optionalMetaDecodeFunc func(data []byte) (err error)
}

type BeginLoadQueryEvent struct {
	FileID    uint32
	BlockData []byte
}

type RowsEvent struct {
	// 0, 1, 2
	Version int

	tableIDSize int
	tables      map[uint64]*TableMapEvent
	needBitmap2 bool

	// for mariadb *_COMPRESSED_EVENT_V1
	compressed bool

	eventType EventType

	Table *TableMapEvent

	TableID uint64

	Flags uint16

	// if version == 2
	// Use when DataLen value is greater than 2
	NdbFormat byte
	NdbData   []byte

	PartitionId       uint16
	SourcePartitionId uint16

	// lenenc_int
	ColumnCount uint64

	/*
		By default MySQL and MariaDB log the full row image.
		see
			- https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_image
			- https://mariadb.com/kb/en/replication-and-binary-log-system-variables/#binlog_row_image

		ColumnBitmap1, ColumnBitmap2 and SkippedColumns are not set on the full row image.
	*/

	// len = (ColumnCount + 7) / 8
	ColumnBitmap1 []byte

	// if UPDATE_ROWS_EVENTv1 or v2, or PARTIAL_UPDATE_ROWS_EVENT
	// len = (ColumnCount + 7) / 8
	ColumnBitmap2 []byte

	// rows: all return types from RowsEvent.decodeValue()
	Rows           [][]interface{}
	SkippedColumns [][]int

	parseTime               bool
	timestampStringLocation *time.Location
	useDecimal              bool
	ignoreJSONDecodeErr     bool
}

// we don't parse all event, so some we will use GenericEvent instead
type GenericEvent struct {
	Data []byte
}

type BinlogParser struct {
	format *FormatDescriptionEvent

	tables map[uint64]*TableMapEvent

	// for rawMode, we only parse FormatDescriptionEvent and RotateEvent
	rawMode bool

	parseTime               bool
	timestampStringLocation *time.Location

	// used to start/stop processing
	stopProcessing uint32

	useDecimal          bool
	ignoreJSONDecodeErr bool
	verifyChecksum      bool

	rowsEventDecodeFunc func(*RowsEvent, []byte) error

	tableMapOptionalMetaDecodeFunc func([]byte) error
}

// Position for binlog filename + position based replication
type Position struct {
	Name string
	Pos  uint32
}

type RConn struct {
	net.Conn

	// Buffered reader for net.Conn in Non-TLS connection only to address replication performance issue.
	// See https://github.com/go-mysql-org/go-mysql/pull/422 for more details.
	br     *bufio.Reader
	reader io.Reader

	copyNBuf []byte

	header [4]byte

	Sequence uint8

	user     string
	password string
	db       string
	proto    string

	// The buffer size to use in the packet connection
	BufferSize int

	serverVersion string
	// server capabilities
	capability uint32
	// client-set capabilities only
	ccaps uint32

	attributes map[string]string

	status uint16

	charset string
	// sets the collation to be set on the auth handshake, this does not issue a 'set names' command
	collation string

	salt           []byte
	authPluginName string

	connectionID uint32
}

// BinlogSyncer syncs binlog events from the server.
type BinlogSyncer struct {
	m sync.RWMutex

	cfg BinlogSyncerConfig

	c *RConn

	wg sync.WaitGroup

	parser *BinlogParser

	nextPos Position

	prevGset, currGset GTIDSet

	// instead of GTIDSet.Clone, use this to speed up calculate prevGset
	prevMySQLGTIDEvent *GTIDEvent

	running bool

	ctx    context.Context
	cancel context.CancelFunc

	lastConnectionID uint32

	retryCount int
}

// Like MySQL GTID Interval struct, [start, stop), left closed and right open
// See MySQL rpl_gtid.h
type Interval struct {
	// The first GID of this interval.
	Start int64
	// The first GID after this interval.
	Stop int64
}

type UUIDSet struct {
	SID uuid.UUID

	Intervals IntervalSlice
}

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

type BinlogEvent struct {
	// raw binlog data which contains all data, including binlog header and event body, and including crc32 checksum if exists
	RawData []byte

	Header *EventHeader
	Event  Event
}

// BinlogStreamer gets the streaming event.
type BinlogStreamer struct {
	ch  chan *BinlogEvent
	ech chan error
	err error
}

type Field struct {
	Data         []byte
	Schema       []byte
	Table        []byte
	OrgTable     []byte
	Name         []byte
	OrgName      []byte
	Charset      uint16
	ColumnLength uint32
	Type         uint8
	Flag         uint16
	Decimal      uint8

	DefaultValueLength uint64
	DefaultValue       []byte
}

type FieldValue struct {
	Type  uint8
	value uint64 // Also for int64 and float64
	str   []byte
}

type Resultset struct {
	Fields     []*Field
	FieldNames map[string]int
	Values     [][]FieldValue

	RawPkg []byte

	RowDatas []RowData

	Streaming     int
	StreamingDone bool
}

type Result struct {
	Status   uint16
	Warnings uint16

	InsertId     uint64
	AffectedRows uint64

	*Resultset
}

type Event interface {
	//Dump Event, format like python-mysql-replication
	Dump(w io.Writer)

	Decode(data []byte) error
}

func (e EventType) String() string {
	switch e {
	case UNKNOWN_EVENT:
		return "UnknownEvent"
	case START_EVENT_V3:
		return "StartEventV3"
	case QUERY_EVENT:
		return "QueryEvent"
	case STOP_EVENT:
		return "StopEvent"
	case ROTATE_EVENT:
		return "RotateEvent"
	case INTVAR_EVENT:
		return "IntVarEvent"
	case LOAD_EVENT:
		return "LoadEvent"
	case SLAVE_EVENT:
		return "SlaveEvent"
	case CREATE_FILE_EVENT:
		return "CreateFileEvent"
	case APPEND_BLOCK_EVENT:
		return "AppendBlockEvent"
	case EXEC_LOAD_EVENT:
		return "ExecLoadEvent"
	case DELETE_FILE_EVENT:
		return "DeleteFileEvent"
	case NEW_LOAD_EVENT:
		return "NewLoadEvent"
	case RAND_EVENT:
		return "RandEvent"
	case USER_VAR_EVENT:
		return "UserVarEvent"
	case FORMAT_DESCRIPTION_EVENT:
		return "FormatDescriptionEvent"
	case XID_EVENT:
		return "XIDEvent"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BeginLoadQueryEvent"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "ExectueLoadQueryEvent"
	case TABLE_MAP_EVENT:
		return "TableMapEvent"
	case WRITE_ROWS_EVENTv0:
		return "WriteRowsEventV0"
	case UPDATE_ROWS_EVENTv0:
		return "UpdateRowsEventV0"
	case DELETE_ROWS_EVENTv0:
		return "DeleteRowsEventV0"
	case WRITE_ROWS_EVENTv1:
		return "WriteRowsEventV1"
	case UPDATE_ROWS_EVENTv1:
		return "UpdateRowsEventV1"
	case DELETE_ROWS_EVENTv1:
		return "DeleteRowsEventV1"
	case INCIDENT_EVENT:
		return "IncidentEvent"
	case HEARTBEAT_EVENT:
		return "HeartbeatEvent"
	case IGNORABLE_EVENT:
		return "IgnorableEvent"
	case ROWS_QUERY_EVENT:
		return "RowsQueryEvent"
	case WRITE_ROWS_EVENTv2:
		return "WriteRowsEventV2"
	case UPDATE_ROWS_EVENTv2:
		return "UpdateRowsEventV2"
	case DELETE_ROWS_EVENTv2:
		return "DeleteRowsEventV2"
	case GTID_EVENT:
		return "GTIDEvent"
	case ANONYMOUS_GTID_EVENT:
		return "AnonymousGTIDEvent"
	case PREVIOUS_GTIDS_EVENT:
		return "PreviousGTIDsEvent"
	case TRANSACTION_CONTEXT_EVENT:
		return "TransactionContextEvent"
	case VIEW_CHANGE_EVENT:
		return "ViewChangeEvent"
	case XA_PREPARE_LOG_EVENT:
		return "XAPrepareLogEvent"
	case PARTIAL_UPDATE_ROWS_EVENT:
		return "PartialUpdateRowsEvent"
	case TRANSACTION_PAYLOAD_EVENT:
		return "TransactionPayloadEvent"
	case HEARTBEAT_LOG_EVENT_V2:
		return "HeartbeatLogEventV2"
	case GTID_TAGGED_LOG_EVENT:
		return "Gtid_tagged_log_event"

	default:
		return "UnknownEvent"
	}
}

func (p Position) String() string {
	return fmt.Sprintf("(%s, %d)", p.Name, p.Pos)
}

func (e *EventError) Error() string {
	return fmt.Sprintf("Header %#v, Data %q, Err: %v", e.Header, e.Data, e.Err)
}

func (e *GenericEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Event data: \n%s", hex.Dump(e.Data))
	fmt.Fprintln(w)
}

func (e *GenericEvent) Decode(data []byte) error {
	e.Data = data

	return nil
}

func (e *TransactionPayloadEvent) compressionType() string {
	switch e.CompressionType {
	case ZSTD:
		return "ZSTD"
	case NONE:
		return "NONE"
	default:
		return "Unknown"
	}
}

func (e *TransactionPayloadEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Payload Size: %d\n", e.Size)
	fmt.Fprintf(w, "Payload Uncompressed Size: %d\n", e.UncompressedSize)
	fmt.Fprintf(w, "Payload CompressionType: %s\n", e.compressionType())
	fmt.Fprintf(w, "Payload Body: \n%s", hex.Dump(e.Payload))
	fmt.Fprintln(w, "=== Start of events decoded from compressed payload ===")
	for _, event := range e.Events {
		event.Dump(w)
	}
	fmt.Fprintln(w, "=== End of events decoded from compressed payload ===")
	fmt.Fprintln(w)
}

func (e *TransactionPayloadEvent) Decode(data []byte) error {
	err := e.decodeFields(data)
	if err != nil {
		return err
	}
	return e.decodePayload()
}

func (e *TransactionPayloadEvent) decodeFields(data []byte) error {
	offset := uint64(0)

	for {
		fieldType := FixedLengthInt(data[offset : offset+1])
		offset++

		if fieldType == OTW_PAYLOAD_HEADER_END_MARK {
			e.Payload = data[offset:]
			break
		} else {
			fieldLength := FixedLengthInt(data[offset : offset+1])
			offset++

			switch fieldType {
			case OTW_PAYLOAD_SIZE_FIELD:
				e.Size = FixedLengthInt(data[offset : offset+fieldLength])
			case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
				e.CompressionType = FixedLengthInt(data[offset : offset+fieldLength])
			case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
				e.UncompressedSize = FixedLengthInt(data[offset : offset+fieldLength])
			}

			offset += fieldLength
		}
	}

	return nil
}

func NewBinlogParser() *BinlogParser {
	p := new(BinlogParser)

	p.tables = make(map[uint64]*TableMapEvent)

	return p
}

func (e *TransactionPayloadEvent) decodePayload() error {
	if e.CompressionType != ZSTD {
		return moerr.NewInternalErrorf(context.Background(), "TransactionPayloadEvent has compression type %d (%s)",
			e.CompressionType, e.compressionType())
	}

	var decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return err
	}
	defer decoder.Close()

	payloadUncompressed, err := decoder.DecodeAll(e.Payload, nil)
	if err != nil {
		return err
	}

	// The uncompressed data needs to be split up into individual events for Parse()
	// to work on them. We can't use e.parser directly as we need to disable checksums
	// but we still need the initialization from the FormatDescriptionEvent. We can't
	// modify e.parser as it is used elsewhere.
	parser := NewBinlogParser()
	parser.format = &FormatDescriptionEvent{
		Version:                e.format.Version,
		ServerVersion:          e.format.ServerVersion,
		CreateTimestamp:        e.format.CreateTimestamp,
		EventHeaderLength:      e.format.EventHeaderLength,
		EventTypeHeaderLengths: e.format.EventTypeHeaderLengths,
		ChecksumAlgorithm:      BINLOG_CHECKSUM_ALG_OFF,
	}

	offset := uint32(0)
	for {
		payloadUncompressedLength := uint32(len(payloadUncompressed))
		if offset+13 > payloadUncompressedLength {
			break
		}
		eventLength := binary.LittleEndian.Uint32(payloadUncompressed[offset+9 : offset+13])
		if offset+eventLength > payloadUncompressedLength {
			return moerr.NewInternalErrorf(context.Background(), "Event length of %d with offset %d in uncompressed payload exceeds payload length of %d",
				eventLength, offset, payloadUncompressedLength)
		}
		data := payloadUncompressed[offset : offset+eventLength]

		pe, err := parser.Parse(data)
		if err != nil {
			return err
		}
		e.Events = append(e.Events, pe)

		offset += eventLength
	}

	return nil
}

func (i *IntVarEvent) Decode(data []byte) error {
	i.Type = data[0]
	i.Value = binary.LittleEndian.Uint64(data[1:])
	return nil
}

func (i *IntVarEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Type: %d\n", i.Type)
	fmt.Fprintf(w, "Value: %d\n", i.Value)
}

func (e *PreviousGTIDsEvent) Decode(data []byte) error {
	pos := 0
	uuidCount := binary.LittleEndian.Uint16(data[pos : pos+8])
	pos += 8

	previousGTIDSets := make([]string, uuidCount)
	for i := range previousGTIDSets {
		uuid := e.decodeUuid(data[pos : pos+16])
		pos += 16
		sliceCount := binary.LittleEndian.Uint16(data[pos : pos+8])
		pos += 8
		intervals := make([]string, sliceCount)
		for i := range intervals {
			start := e.decodeInterval(data[pos : pos+8])
			pos += 8
			stop := e.decodeInterval(data[pos : pos+8])
			pos += 8
			interval := ""
			if stop == start+1 {
				interval = fmt.Sprintf("%d", start)
			} else {
				interval = fmt.Sprintf("%d-%d", start, stop-1)
			}
			intervals[i] = interval
		}
		previousGTIDSets[i] = fmt.Sprintf("%s:%s", uuid, strings.Join(intervals, ":"))
	}
	e.GTIDSets = strings.Join(previousGTIDSets, ",")
	return nil
}

func (e *PreviousGTIDsEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Previous GTID Event: %s\n", e.GTIDSets)
	fmt.Fprintln(w)
}

func (e *PreviousGTIDsEvent) decodeUuid(data []byte) string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(data[0:4]), hex.EncodeToString(data[4:6]),
		hex.EncodeToString(data[6:8]), hex.EncodeToString(data[8:10]), hex.EncodeToString(data[10:]))
}

func (e *PreviousGTIDsEvent) decodeInterval(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func (e *ExecuteLoadQueryEvent) Decode(data []byte) error {
	pos := 0

	e.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.SchemaLength = data[pos]
	pos++

	e.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.StatusVars = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.FileID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.StartPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.EndPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.DupHandlingFlags = data[pos]

	return nil
}

func (e *ExecuteLoadQueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Slave proxy ID: %d\n", e.SlaveProxyID)
	fmt.Fprintf(w, "Execution time: %d\n", e.ExecutionTime)
	fmt.Fprintf(w, "Schame length: %d\n", e.SchemaLength)
	fmt.Fprintf(w, "Error code: %d\n", e.ErrorCode)
	fmt.Fprintf(w, "Status vars length: %d\n", e.StatusVars)
	fmt.Fprintf(w, "File ID: %d\n", e.FileID)
	fmt.Fprintf(w, "Start pos: %d\n", e.StartPos)
	fmt.Fprintf(w, "End pos: %d\n", e.EndPos)
	fmt.Fprintf(w, "Dup handling flags: %d\n", e.DupHandlingFlags)
	fmt.Fprintln(w)
}

func (e *BeginLoadQueryEvent) Decode(data []byte) error {
	pos := 0

	e.FileID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.BlockData = data[pos:]

	return nil
}

func (e *BeginLoadQueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "File ID: %d\n", e.FileID)
	fmt.Fprintf(w, "Block data: %s\n", e.BlockData)
	fmt.Fprintln(w)
}

func (e *GTIDEvent) Decode(data []byte) error {
	pos := 0
	e.CommitFlag = data[pos]
	pos++
	e.SID = data[pos : pos+SidLength]
	pos += SidLength
	e.GNO = int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	if len(data) >= 42 {
		if data[pos] == LogicalTimestampTypeCode {
			pos++
			e.LastCommitted = int64(binary.LittleEndian.Uint64(data[pos:]))
			pos += PartLogicalTimestampLength
			e.SequenceNumber = int64(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8

			// IMMEDIATE_COMMIT_TIMESTAMP_LENGTH = 7
			if len(data)-pos < 7 {
				return nil
			}
			e.ImmediateCommitTimestamp = FixedLengthInt(data[pos : pos+7])
			pos += 7
			if (e.ImmediateCommitTimestamp & (uint64(1) << 55)) != 0 {
				// If the most significant bit set, another 7 byte follows representing OriginalCommitTimestamp
				e.ImmediateCommitTimestamp &= ^(uint64(1) << 55)
				e.OriginalCommitTimestamp = FixedLengthInt(data[pos : pos+7])
				pos += 7
			} else {
				// Otherwise OriginalCommitTimestamp == ImmediateCommitTimestamp
				e.OriginalCommitTimestamp = e.ImmediateCommitTimestamp
			}

			// TRANSACTION_LENGTH_MIN_LENGTH = 1
			if len(data)-pos < 1 {
				return nil
			}
			var n int
			e.TransactionLength, _, n = LengthEncodedInt(data[pos:])
			pos += n

			// IMMEDIATE_SERVER_VERSION_LENGTH = 4
			e.ImmediateServerVersion = UndefinedServerVer
			e.OriginalServerVersion = UndefinedServerVer
			if len(data)-pos < 4 {
				return nil
			}
			e.ImmediateServerVersion = binary.LittleEndian.Uint32(data[pos:])
			pos += 4
			if (e.ImmediateServerVersion & (uint32(1) << 31)) != 0 {
				// If the most significant bit set, another 4 byte follows representing OriginalServerVersion
				e.ImmediateServerVersion &= ^(uint32(1) << 31)
				e.OriginalServerVersion = binary.LittleEndian.Uint32(data[pos:])
				// pos += 4
			} else {
				// Otherwise OriginalServerVersion == ImmediateServerVersion
				e.OriginalServerVersion = e.ImmediateServerVersion
			}
		}
	}
	return nil
}

func microSecTimestampToTime(ts uint64) time.Time {
	if ts == 0 {
		return time.Time{}
	}
	return time.Unix(int64(ts/1000000), int64(ts%1000000)*1000)
}

// ImmediateCommitTime returns the commit time of this trx on the immediate server
// or zero time if not available.
func (e *GTIDEvent) ImmediateCommitTime() time.Time {
	return microSecTimestampToTime(e.ImmediateCommitTimestamp)
}

// OriginalCommitTime returns the commit time of this trx on the original server
// or zero time if not available.
func (e *GTIDEvent) OriginalCommitTime() time.Time {
	return microSecTimestampToTime(e.OriginalCommitTimestamp)
}

func (e *GTIDEvent) Dump(w io.Writer) {
	fmtTime := func(t time.Time) string {
		if t.IsZero() {
			return "<n/a>"
		}
		return t.Format(time.RFC3339Nano)
	}

	fmt.Fprintf(w, "Commit flag: %d\n", e.CommitFlag)
	u, _ := uuid.FromBytes(e.SID)
	fmt.Fprintf(w, "GTID_NEXT: %s:%d\n", u.String(), e.GNO)
	fmt.Fprintf(w, "LAST_COMMITTED: %d\n", e.LastCommitted)
	fmt.Fprintf(w, "SEQUENCE_NUMBER: %d\n", e.SequenceNumber)
	fmt.Fprintf(w, "Immediate commmit timestamp: %d (%s)\n", e.ImmediateCommitTimestamp, fmtTime(e.ImmediateCommitTime()))
	fmt.Fprintf(w, "Orignal commmit timestamp: %d (%s)\n", e.OriginalCommitTimestamp, fmtTime(e.OriginalCommitTime()))
	fmt.Fprintf(w, "Transaction length: %d\n", e.TransactionLength)
	fmt.Fprintf(w, "Immediate server version: %d\n", e.ImmediateServerVersion)
	fmt.Fprintf(w, "Orignal server version: %d\n", e.OriginalServerVersion)
	fmt.Fprintln(w)
}

func (e *RowsQueryEvent) Decode(data []byte) error {
	// ignore length byte 1
	e.Query = data[1:]
	return nil
}

func (e *RowsQueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}

func (e *RowsEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Column count: %d\n", e.ColumnCount)
	fmt.Fprintf(w, "NDB data: %s\n", e.NdbData)

	fmt.Fprintf(w, "Values:\n")
	for _, rows := range e.Rows {
		fmt.Fprintf(w, "--\n")
		for j, d := range rows {
			switch dt := d.(type) {
			case []byte:
				fmt.Fprintf(w, "%d:%q\n", j, dt)
			case *JsonDiff:
				fmt.Fprintf(w, "%d:%s\n", j, dt)
			default:
				fmt.Fprintf(w, "%d:%#v\n", j, d)
			}
		}
	}
	fmt.Fprintln(w)
}

func (e *RowsEvent) decodeExtraData(data []byte) (err2 error) {
	pos := 0
	extraDataType := data[pos]
	pos += 1
	switch extraDataType {
	case ENUM_EXTRA_ROW_INFO_TYPECODE_NDB:
		var ndbLength int = int(data[pos])
		pos += 1
		e.NdbFormat = data[pos]
		pos += 1
		e.NdbData = data[pos : pos+ndbLength-2]
	case ENUM_EXTRA_ROW_INFO_TYPECODE_PARTITION:
		if e.eventType == UPDATE_ROWS_EVENTv1 || e.eventType == UPDATE_ROWS_EVENTv2 || e.eventType == PARTIAL_UPDATE_ROWS_EVENT {
			e.PartitionId = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
			e.SourcePartitionId = binary.LittleEndian.Uint16(data[pos:])
		} else {
			e.PartitionId = binary.LittleEndian.Uint16(data[pos:])
		}
	}
	return nil
}

func (e *RowsEvent) DecodeHeader(data []byte) (int, error) {
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.tableIDSize])
	pos += e.tableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if e.Version == 2 {
		dataLen := binary.LittleEndian.Uint16(data[pos:])
		pos += 2
		if dataLen > 2 {
			err := e.decodeExtraData(data[pos:])
			if err != nil {
				return 0, err
			}
		}
		pos += int(dataLen - 2)
	}

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	bitCount := bitmapByteSize(int(e.ColumnCount))
	e.ColumnBitmap1 = data[pos : pos+bitCount]
	pos += bitCount

	if e.needBitmap2 {
		e.ColumnBitmap2 = data[pos : pos+bitCount]
		pos += bitCount
	}

	var ok bool
	e.Table, ok = e.tables[e.TableID]
	if !ok {
		if len(e.tables) > 0 {
			return 0, moerr.NewInternalErrorf(context.Background(), "invalid table id %d, no corresponding table map event", e.TableID)
		} else {
			return 0, moerr.NewInternalErrorf(context.Background(), "invalid table id, no corresponding table map event, table id %d", e.TableID)
		}
	}
	return pos, nil
}

// JsonColumnCount returns the number of JSON columns in this table
func (e *TableMapEvent) JsonColumnCount() uint64 {
	count := uint64(0)
	for _, t := range e.ColumnType {
		if t == MYSQL_TYPE_JSON {
			count++
		}
	}

	return count
}

func isBitSet(bitmap []byte, i int) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}

func isBitSetIncr(bitmap []byte, i *int) bool {
	v := isBitSet(bitmap, *i)
	*i++
	return v
}

func ParseBinaryUint24(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
}

func ParseBinaryInt24(data []byte) int32 {
	u32 := ParseBinaryUint24(data)
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return int32(u32)
}

func decodeDecimalDecompressValue(compIndx int, data []byte, mask uint8) (size int, value uint32) {
	size = compressedBytes[compIndx]
	switch size {
	case 0:
	case 1:
		value = uint32(data[0] ^ mask)
	case 2:
		value = uint32(data[1]^mask) | uint32(data[0]^mask)<<8
	case 3:
		value = uint32(data[2]^mask) | uint32(data[1]^mask)<<8 | uint32(data[0]^mask)<<16
	case 4:
		value = uint32(data[3]^mask) | uint32(data[2]^mask)<<8 | uint32(data[1]^mask)<<16 | uint32(data[0]^mask)<<24
	}
	return
}

func decodeDecimal(data []byte, precision int, decimals int, useDecimal bool) (interface{}, int, error) {
	// see python mysql replication and https://github.com/jeremycole/mysql_binlog
	integral := precision - decimals
	uncompIntegral := integral / digitsPerInteger
	uncompFractional := decimals / digitsPerInteger
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	// must copy the data for later change
	data = buf

	// Support negative
	// The sign is encoded in the high bit of the the byte
	// But this bit can also be used in the value
	value := uint32(data[0])
	var res strings.Builder
	res.Grow(precision + 2)
	var mask uint32 = 0
	if value&0x80 == 0 {
		mask = uint32((1 << 32) - 1)
		res.WriteString("-")
	}

	// clear sign
	data[0] ^= 0x80

	zeroLeading := true

	pos, value := decodeDecimalDecompressValue(compIntegral, data, uint8(mask))
	if value != 0 {
		zeroLeading = false
		res.WriteString(strconv.FormatUint(uint64(value), 10))
	}

	for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		if zeroLeading {
			if value != 0 {
				zeroLeading = false
				res.WriteString(strconv.FormatUint(uint64(value), 10))
			}
		} else {
			toWrite := strconv.FormatUint(uint64(value), 10)
			res.Write(zeros[:digitsPerInteger-len(toWrite)])
			res.WriteString(toWrite)
		}
	}

	if zeroLeading {
		res.WriteString("0")
	}

	if pos < len(data) {
		res.WriteString(".")

		for i := 0; i < uncompFractional; i++ {
			value = binary.BigEndian.Uint32(data[pos:]) ^ mask
			pos += 4
			toWrite := strconv.FormatUint(uint64(value), 10)
			res.Write(zeros[:digitsPerInteger-len(toWrite)])
			res.WriteString(toWrite)
		}

		if size, value := decodeDecimalDecompressValue(compFractional, data[pos:], uint8(mask)); size > 0 {
			toWrite := strconv.FormatUint(uint64(value), 10)
			padding := compFractional - len(toWrite)
			if padding > 0 {
				res.Write(zeros[:padding])
			}
			res.WriteString(toWrite)
			pos += size
		}
	}

	if useDecimal {
		f, err := decimal.NewFromString(res.String())
		return f, pos, err
	}

	return res.String(), pos, nil
}

// BFixedLengthInt: big endian
func BFixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
	}
	return num
}

func decodeBit(data []byte, nbits int, length int) (value int64, err error) {
	if nbits > 1 {
		switch length {
		case 1:
			value = int64(data[0])
		case 2:
			value = int64(binary.BigEndian.Uint16(data))
		case 3:
			value = int64(BFixedLengthInt(data[0:3]))
		case 4:
			value = int64(binary.BigEndian.Uint32(data))
		case 5:
			value = int64(BFixedLengthInt(data[0:5]))
		case 6:
			value = int64(BFixedLengthInt(data[0:6]))
		case 7:
			value = int64(BFixedLengthInt(data[0:7]))
		case 8:
			value = int64(binary.BigEndian.Uint64(data))
		default:
			err = moerr.NewInternalErrorf(context.Background(), "invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = moerr.NewInternalErrorf(context.Background(), "invalid bit length %d", length)
		} else {
			value = int64(data[0])
		}
	}
	return
}

func formatZeroTime(frac int, dec int) string {
	if dec == 0 {
		return "0000-00-00 00:00:00"
	}

	s := fmt.Sprintf("0000-00-00 00:00:00.%06d", frac)

	// dec must < 6, if frac is 924000, but dec is 3, we must output 924 here.
	return s[0 : len(s)-(6-dec)]
}

func (e *RowsEvent) parseFracTime(t interface{}) interface{} {
	v, ok := t.(fracTime)
	if !ok {
		return t
	}

	if !e.parseTime {
		// Don't parse time, return string directly
		return v.String()
	}

	// return Golang time directly
	return v.Time
}

func formatBeforeUnixZeroTime(year, month, day, hour, minute, second, frac, dec int) string {
	if dec == 0 {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	}

	s := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)

	// dec must < 6, if frac is 924000, but dec is 3, we must output 924 here.
	return s[0 : len(s)-(6-dec)]
}

func decodeDatetime2(data []byte, dec uint16) (interface{}, int, error) {
	// get datetime binary length
	n := int(5 + (dec+1)/2)

	intPart := int64(BFixedLengthInt(data[0:5])) - DATETIMEF_INT_OFS
	var frac int64 = 0

	switch dec {
	case 1, 2:
		frac = int64(data[5]) * 10000
	case 3, 4:
		frac = int64(binary.BigEndian.Uint16(data[5:7])) * 100
	case 5, 6:
		frac = int64(BFixedLengthInt(data[5:8]))
	}

	if intPart == 0 {
		return formatZeroTime(int(frac), int(dec)), n, nil
	}

	tmp := intPart<<24 + frac
	// handle sign???
	if tmp < 0 {
		tmp = -tmp
	}

	// var secPart int64 = tmp % (1 << 24)
	ymdhms := tmp >> 24

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)

	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int(hms >> 12)

	// DATETIME encoding for nonfractional part after MySQL 5.6.4
	// https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
	// integer value for 1970-01-01 00:00:00 is
	// year*13+month = 25611 = 0b110010000001011
	// day = 1 = 0b00001
	// hour = 0 = 0b00000
	// minute = 0 = 0b000000
	// second = 0 = 0b000000
	// integer value = 0b1100100000010110000100000000000000000 = 107420450816
	if intPart < 107420450816 {
		return formatBeforeUnixZeroTime(year, month, day, hour, minute, second, int(frac), int(dec)), n, nil
	}

	return fracTime{
		Time: time.Date(year, time.Month(month), day, hour, minute, second, int(frac*1000), time.UTC),
		Dec:  int(dec),
	}, n, nil
}

func timeFormat(tmp int64, dec uint16, n int) (string, int, error) {
	hms := int64(0)
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}

	hms = tmp >> 24

	hour := (hms >> 12) % (1 << 10) /* 10 bits starting at 12th */
	minute := (hms >> 6) % (1 << 6) /* 6 bits starting at 6th   */
	second := hms % (1 << 6)        /* 6 bits starting at 0th   */
	secPart := tmp % (1 << 24)

	if secPart != 0 {
		s := fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, secPart)
		return s[0 : len(s)-(6-int(dec))], n, nil
	}

	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second), n, nil
}

func decodeTime2(data []byte, dec uint16) (string, int, error) {
	// time  binary length
	n := int(3 + (dec+1)/2)

	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)
	switch dec {
	case 1, 2:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(data[3])
		if intPart < 0 && frac != 0 {
			/*
			   Negative values are stored with reverse fractional part order,
			   for binary sort compatibility.

			     Disk value  intpart frac   Time value   Memory value
			     800000.00    0      0      00:00:00.00  0000000000.000000
			     7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
			     7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
			     7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
			     7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
			     7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

			     Formula to convert fractional part from disk format
			     (now stored in "frac" variable) to absolute value: "0x100 - frac".
			     To reconstruct in-memory value, we shift
			     to the next integer value and then substruct fractional part.
			*/
			intPart++     /* Shift to the next integer value */
			frac -= 0x100 /* -(0x100 - frac) */
		}
		tmp = intPart<<24 + frac*10000
	case 3, 4:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac != 0 {
			/*
			   Fix reverse fractional part order: "0x10000 - frac".
			   See comments for FSP=1 and FSP=2 above.
			*/
			intPart++       /* Shift to the next integer value */
			frac -= 0x10000 /* -(0x10000-frac) */
		}
		tmp = intPart<<24 + frac*100

	case 5, 6:
		tmp = int64(BFixedLengthInt(data[0:6])) - TIMEF_OFS
		return timeFormat(tmp, dec, n)
	default:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		tmp = intPart << 24
	}

	if intPart == 0 && frac == 0 {
		return "00:00:00", n, nil
	}

	return timeFormat(tmp, dec, n)
}

func decodeTimestamp2(data []byte, dec uint16, timestampStringLocation *time.Location) (interface{}, int, error) {
	// get timestamp binary length
	n := int(4 + (dec+1)/2)
	sec := int64(binary.BigEndian.Uint32(data[0:4]))
	usec := int64(0)
	switch dec {
	case 1, 2:
		usec = int64(data[4]) * 10000
	case 3, 4:
		usec = int64(binary.BigEndian.Uint16(data[4:])) * 100
	case 5, 6:
		usec = int64(BFixedLengthInt(data[4:7]))
	}

	if sec == 0 {
		return formatZeroTime(int(usec), int(dec)), n, nil
	}

	return fracTime{
		Time:                    time.Unix(sec, usec*1000),
		Dec:                     int(dec),
		timestampStringLocation: timestampStringLocation,
	}, n, nil
}

func littleDecodeBit(data []byte, nbits int, length int) (value int64, err error) {
	if nbits > 1 {
		switch length {
		case 1:
			value = int64(data[0])
		case 2:
			value = int64(binary.LittleEndian.Uint16(data))
		case 3:
			value = int64(FixedLengthInt(data[0:3]))
		case 4:
			value = int64(binary.LittleEndian.Uint32(data))
		case 5:
			value = int64(FixedLengthInt(data[0:5]))
		case 6:
			value = int64(FixedLengthInt(data[0:6]))
		case 7:
			value = int64(FixedLengthInt(data[0:7]))
		case 8:
			value = int64(binary.LittleEndian.Uint64(data))
		default:
			err = moerr.NewInternalErrorf(context.Background(), "invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = moerr.NewInternalErrorf(context.Background(), "invalid bit length %d", length)
		} else {
			value = int64(data[0])
		}
	}
	return
}

func decodeBlob(data []byte, meta uint16) (v []byte, n int, err error) {
	var length int
	switch meta {
	case 1:
		length = int(data[0])
		v = data[1 : 1+length]
		n = length + 1
	case 2:
		length = int(binary.LittleEndian.Uint16(data))
		v = data[2 : 2+length]
		n = length + 2
	case 3:
		length = int(FixedLengthInt(data[0:3]))
		v = data[3 : 3+length]
		n = length + 3
	case 4:
		length = int(binary.LittleEndian.Uint32(data))
		v = data[4 : 4+length]
		n = length + 4
	default:
		err = moerr.NewInternalErrorf(context.Background(), "invalid blob packlen = %d", meta)
	}

	return
}

func decodeString(data []byte, length int) (v string, n int) {
	if length < 256 {
		length = int(data[0])

		n = length + 1
		v = hack.String(data[1:n])
	} else {
		length = int(binary.LittleEndian.Uint16(data[0:]))
		n = length + 2
		v = hack.String(data[2:n])
	}

	return
}

func (d *jsonBinaryDecoder) isDataShort(data []byte, expected int) bool {
	if d.err != nil {
		return true
	}

	if len(data) < expected {
		d.err = moerr.NewInternalErrorf(context.Background(), "data len %d < expected %d", len(data), expected)
	}

	return d.err != nil
}

func jsonbGetOffsetSize(isSmall bool) int {
	if isSmall {
		return jsonbSmallOffsetSize
	}

	return jsonbLargeOffsetSize
}

func (d *jsonBinaryDecoder) decodeUint16(data []byte) uint16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := ParseBinaryUint16(data[0:2])
	return v
}

func (d *jsonBinaryDecoder) decodeUint32(data []byte) uint32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := ParseBinaryUint32(data[0:4])
	return v
}

func (d *jsonBinaryDecoder) decodeCount(data []byte, isSmall bool) int {
	if isSmall {
		v := d.decodeUint16(data)
		return int(v)
	}

	return int(d.decodeUint32(data))
}

func jsonbGetKeyEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbKeyEntrySizeSmall
	}

	return jsonbKeyEntrySizeLarge
}

func jsonbGetValueEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbValueEntrySizeSmall
	}

	return jsonbValueEntrySizeLarge
}

func isInlineValue(tp byte, isSmall bool) bool {
	switch tp {
	case JSONB_INT16, JSONB_UINT16, JSONB_LITERAL:
		return true
	case JSONB_INT32, JSONB_UINT32:
		return !isSmall
	}

	return false
}

func (d *jsonBinaryDecoder) decodeObjectOrArray(data []byte, isSmall bool, isObject bool) interface{} {
	offsetSize := jsonbGetOffsetSize(isSmall)
	if d.isDataShort(data, 2*offsetSize) {
		return nil
	}

	count := d.decodeCount(data, isSmall)
	size := d.decodeCount(data[offsetSize:], isSmall)

	if d.isDataShort(data, size) {
		// Before MySQL 5.7.22, json type generated column may have invalid value,
		// bug ref: https://bugs.mysql.com/bug.php?id=88791
		// As generated column value is not used in replication, we can just ignore
		// this error and return a dummy value for this column.
		if d.ignoreDecodeErr {
			d.err = nil
		}
		return nil
	}

	keyEntrySize := jsonbGetKeyEntrySize(isSmall)
	valueEntrySize := jsonbGetValueEntrySize(isSmall)

	headerSize := 2*offsetSize + count*valueEntrySize

	if isObject {
		headerSize += count * keyEntrySize
	}

	if headerSize > size {
		d.err = moerr.NewInternalErrorf(context.Background(), "header size %d > size %d", headerSize, size)
		return nil
	}

	var keys []string
	if isObject {
		keys = make([]string, count)
		for i := 0; i < count; i++ {
			// decode key
			entryOffset := 2*offsetSize + keyEntrySize*i
			keyOffset := d.decodeCount(data[entryOffset:], isSmall)
			keyLength := int(d.decodeUint16(data[entryOffset+offsetSize:]))

			// Key must start after value entry
			if keyOffset < headerSize {
				d.err = moerr.NewInternalErrorf(context.Background(), "invalid key offset %d, must > %d", keyOffset, headerSize)
				return nil
			}

			if d.isDataShort(data, keyOffset+keyLength) {
				return nil
			}

			keys[i] = hack.String(data[keyOffset : keyOffset+keyLength])
		}
	}

	if d.err != nil {
		return nil
	}

	values := make([]interface{}, count)
	for i := 0; i < count; i++ {
		// decode value
		entryOffset := 2*offsetSize + valueEntrySize*i
		if isObject {
			entryOffset += keyEntrySize * count
		}

		tp := data[entryOffset]

		if isInlineValue(tp, isSmall) {
			values[i] = d.decodeValue(tp, data[entryOffset+1:entryOffset+valueEntrySize])
			continue
		}

		valueOffset := d.decodeCount(data[entryOffset+1:], isSmall)

		if d.isDataShort(data, valueOffset) {
			return nil
		}

		values[i] = d.decodeValue(tp, data[valueOffset:])
	}

	if d.err != nil {
		return nil
	}

	if !isObject {
		return values
	}

	m := make(map[string]interface{}, count)
	for i := 0; i < count; i++ {
		m[keys[i]] = values[i]
	}

	return m
}

func (d *jsonBinaryDecoder) decodeLiteral(data []byte) interface{} {
	if d.isDataShort(data, 1) {
		return nil
	}

	tp := data[0]

	switch tp {
	case JSONB_NULL_LITERAL:
		return nil
	case JSONB_TRUE_LITERAL:
		return true
	case JSONB_FALSE_LITERAL:
		return false
	}

	d.err = moerr.NewInternalErrorf(context.Background(), "invalid literal %c", tp)

	return nil
}

func (d *jsonBinaryDecoder) decodeInt16(data []byte) int16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := ParseBinaryInt16(data[0:2])
	return v
}

func (d *jsonBinaryDecoder) decodeInt32(data []byte) int32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := ParseBinaryInt32(data[0:4])
	return v
}

func (d *jsonBinaryDecoder) decodeInt64(data []byte) int64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := ParseBinaryInt64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) decodeUint64(data []byte) uint64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := ParseBinaryUint64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) decodeDouble(data []byte) float64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := ParseBinaryFloat64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) decodeVariableLength(data []byte) (int, int) {
	// The max size for variable length is math.MaxUint32, so
	// here we can use 5 bytes to save it.
	maxCount := 5
	if len(data) < maxCount {
		maxCount = len(data)
	}

	pos := 0
	length := uint64(0)
	for ; pos < maxCount; pos++ {
		v := data[pos]
		length |= uint64(v&0x7F) << uint(7*pos)

		if v&0x80 == 0 {
			if length > math.MaxUint32 {
				d.err = moerr.NewInternalErrorf(context.Background(), "variable length %d must <= %d", length, int64(math.MaxUint32))
				return 0, 0
			}

			pos += 1
			// TODO: should consider length overflow int here.
			return int(length), pos
		}
	}

	d.err = moerr.NewInternalError(context.Background(), "decode variable length failed")

	return 0, 0
}

func (d *jsonBinaryDecoder) decodeString(data []byte) string {
	if d.err != nil {
		return ""
	}

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		return ""
	}

	data = data[n:]

	v := hack.String(data[0:l])
	return v
}

func (d *jsonBinaryDecoder) decodeDecimal(data []byte) interface{} {
	precision := int(data[0])
	scale := int(data[1])

	v, _, err := decodeDecimal(data[2:], precision, scale, d.useDecimal)
	d.err = err

	return v
}

func (d *jsonBinaryDecoder) decodeTime(data []byte) interface{} {
	v := d.decodeInt64(data)

	if v == 0 {
		return "00:00:00"
	}

	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}

	intPart := v >> 24
	hour := (intPart >> 12) % (1 << 10)
	min := (intPart >> 6) % (1 << 6)
	sec := intPart % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, min, sec, frac)
}

func (d *jsonBinaryDecoder) decodeDateTime(data []byte) interface{} {
	v := d.decodeInt64(data)
	if v == 0 {
		return "0000-00-00 00:00:00"
	}

	// handle negative?
	if v < 0 {
		v = -v
	}

	intPart := v >> 24
	ymd := intPart >> 17
	ym := ymd >> 5
	hms := intPart % (1 << 17)

	year := ym / 13
	month := ym % 13
	day := ymd % (1 << 5)
	hour := hms >> 12
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)
}

func (d *jsonBinaryDecoder) decodeOpaque(data []byte) interface{} {
	if d.isDataShort(data, 1) {
		return nil
	}

	tp := data[0]
	data = data[1:]

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		return nil
	}

	data = data[n : l+n]

	switch tp {
	case MYSQL_TYPE_NEWDECIMAL:
		return d.decodeDecimal(data)
	case MYSQL_TYPE_TIME:
		return d.decodeTime(data)
	case MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
		return d.decodeDateTime(data)
	default:
		return hack.String(data)
	}
}

func (d *jsonBinaryDecoder) decodeValue(tp byte, data []byte) interface{} {
	if d.err != nil {
		return nil
	}

	switch tp {
	case JSONB_SMALL_OBJECT:
		return d.decodeObjectOrArray(data, true, true)
	case JSONB_LARGE_OBJECT:
		return d.decodeObjectOrArray(data, false, true)
	case JSONB_SMALL_ARRAY:
		return d.decodeObjectOrArray(data, true, false)
	case JSONB_LARGE_ARRAY:
		return d.decodeObjectOrArray(data, false, false)
	case JSONB_LITERAL:
		return d.decodeLiteral(data)
	case JSONB_INT16:
		return d.decodeInt16(data)
	case JSONB_UINT16:
		return d.decodeUint16(data)
	case JSONB_INT32:
		return d.decodeInt32(data)
	case JSONB_UINT32:
		return d.decodeUint32(data)
	case JSONB_INT64:
		return d.decodeInt64(data)
	case JSONB_UINT64:
		return d.decodeUint64(data)
	case JSONB_DOUBLE:
		return d.decodeDouble(data)
	case JSONB_STRING:
		return d.decodeString(data)
	case JSONB_OPAQUE:
		return d.decodeOpaque(data)
	default:
		d.err = moerr.NewInternalErrorf(context.Background(), "invalid json type %d", tp)
	}

	return nil
}

// decodeJsonBinary decodes the JSON binary encoding data and returns
// the common JSON encoding data.
func (e *RowsEvent) decodeJsonBinary(data []byte) ([]byte, error) {
	d := jsonBinaryDecoder{
		useDecimal:      e.useDecimal,
		ignoreDecodeErr: e.ignoreJSONDecodeErr,
	}

	if d.isDataShort(data, 1) {
		return nil, d.err
	}

	v := d.decodeValue(data[0], data[1:])
	if d.err != nil {
		return nil, d.err
	}

	return json.Marshal(v)
}

func (e *RowsEvent) decodeJsonPartialBinary(data []byte) (*JsonDiff, error) {
	// see Json_diff_vector::read_binary() in mysql-server/sql/json_diff.cc
	operationNumber := data[0]
	switch operationNumber {
	case JsonDiffOperationReplace:
	case JsonDiffOperationInsert:
	case JsonDiffOperationRemove:
	default:
		return nil, moerr.NewInternalError(context.Background(), "corrupted JSON diff")
	}
	data = data[1:]

	pathLength, _, n := LengthEncodedInt(data)
	data = data[n:]

	path := data[:pathLength]
	data = data[pathLength:]

	diff := &JsonDiff{
		Op:   operationNumber,
		Path: string(path),
		// Value will be filled below
	}

	if operationNumber == JsonDiffOperationRemove {
		return diff, nil
	}

	valueLength, _, n := LengthEncodedInt(data)
	data = data[n:]

	d, err := e.decodeJsonBinary(data[:valueLength])
	if err != nil {
		return nil, moerr.NewInternalErrorf(context.Background(), "cannot read json diff for field %q: %w", path, err)
	}
	diff.Value = string(d)

	return diff, nil
}

// see mysql sql/log_event.cc log_event_print_value
func (e *RowsEvent) decodeValue(data []byte, tp byte, meta uint16, isPartial bool) (v interface{}, n int, err error) {
	var length = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = b0 | 0x30
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_LONG:
		n = 4
		v = ParseBinaryInt32(data)
	case MYSQL_TYPE_TINY:
		n = 1
		v = ParseBinaryInt8(data)
	case MYSQL_TYPE_SHORT:
		n = 2
		v = ParseBinaryInt16(data)
	case MYSQL_TYPE_INT24:
		n = 3
		v = ParseBinaryInt24(data)
	case MYSQL_TYPE_LONGLONG:
		n = 8
		v = ParseBinaryInt64(data)
	case MYSQL_TYPE_NEWDECIMAL:
		prec := uint8(meta >> 8)
		scale := uint8(meta & 0xFF)
		v, n, err = decodeDecimal(data, int(prec), int(scale), e.useDecimal)
	case MYSQL_TYPE_FLOAT:
		n = 4
		v = ParseBinaryFloat32(data)
	case MYSQL_TYPE_DOUBLE:
		n = 8
		v = ParseBinaryFloat64(data)
	case MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		n = int(nbits+7) / 8

		// use int64 for bit
		v, err = decodeBit(data, int(nbits), n)
	case MYSQL_TYPE_TIMESTAMP:
		n = 4
		t := binary.LittleEndian.Uint32(data)
		if t == 0 {
			v = formatZeroTime(0, 0)
		} else {
			v = e.parseFracTime(fracTime{
				Time:                    time.Unix(int64(t), 0),
				Dec:                     0,
				timestampStringLocation: e.timestampStringLocation,
			})
		}
	case MYSQL_TYPE_TIMESTAMP2:
		v, n, err = decodeTimestamp2(data, meta, e.timestampStringLocation)
		v = e.parseFracTime(v)
	case MYSQL_TYPE_DATETIME:
		n = 8
		i64 := binary.LittleEndian.Uint64(data)
		if i64 == 0 {
			v = formatZeroTime(0, 0)
		} else {
			d := i64 / 1000000
			t := i64 % 1000000
			v = e.parseFracTime(fracTime{
				Time: time.Date(
					int(d/10000),
					time.Month((d%10000)/100),
					int(d%100),
					int(t/10000),
					int((t%10000)/100),
					int(t%100),
					0,
					time.UTC,
				),
				Dec: 0,
			})
		}
	case MYSQL_TYPE_DATETIME2:
		v, n, err = decodeDatetime2(data, meta)
		v = e.parseFracTime(v)
	case MYSQL_TYPE_TIME:
		n = 3
		i32 := uint32(FixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "00:00:00"
		} else {
			v = fmt.Sprintf("%02d:%02d:%02d", i32/10000, (i32%10000)/100, i32%100)
		}
	case MYSQL_TYPE_TIME2:
		v, n, err = decodeTime2(data, meta)
	case MYSQL_TYPE_DATE:
		n = 3
		i32 := uint32(FixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "0000-00-00"
		} else {
			v = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
		}

	case MYSQL_TYPE_YEAR:
		n = 1
		year := int(data[0])
		if year == 0 {
			v = year
		} else {
			v = year + 1900
		}
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			v = int64(data[0])
			n = 1
		case 2:
			v = int64(binary.LittleEndian.Uint16(data))
			n = 2
		default:
			err = moerr.NewInternalErrorf(context.Background(), "Unknown ENUM packlen=%d", l)
		}
	case MYSQL_TYPE_SET:
		n = int(meta & 0xFF)
		nbits := n * 8

		v, err = littleDecodeBit(data, nbits, n)
	case MYSQL_TYPE_BLOB:
		v, n, err = decodeBlob(data, meta)
	case MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING:
		length = int(meta)
		v, n = decodeString(data, length)
	case MYSQL_TYPE_STRING:
		v, n = decodeString(data, length)
	case MYSQL_TYPE_JSON:
		// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java#L404
		length = int(FixedLengthInt(data[0:meta]))
		n = length + int(meta)

		/*
		   See https://github.com/mysql/mysql-server/blob/7b6fb0753b428537410f5b1b8dc60e5ccabc9f70/sql-common/json_binary.cc#L1077

		   Each document should start with a one-byte type specifier, so an
		   empty document is invalid according to the format specification.
		   Empty documents may appear due to inserts using the IGNORE keyword
		   or with non-strict SQL mode, which will insert an empty string if
		   the value NULL is inserted into a NOT NULL column. We choose to
		   interpret empty values as the JSON null literal.

		   In our implementation (go-mysql) for backward compatibility we prefer return empty slice.
		*/
		if length == 0 {
			v = []byte{}
		} else {
			if isPartial {
				var diff *JsonDiff
				diff, err = e.decodeJsonPartialBinary(data[meta:n])
				if err == nil {
					v = diff
				} else {
					fmt.Printf("decodeJsonPartialBinary(%q) fail: %s\n", data[meta:n], err)
				}
			} else {
				var d []byte
				d, err = e.decodeJsonBinary(data[meta:n])
				if err == nil {
					v = hack.String(d)
				}
			}
		}
	case MYSQL_TYPE_GEOMETRY:
		// MySQL saves Geometry as Blob in binlog
		// Seem that the binary format is SRID (4 bytes) + WKB, outer can use
		// MySQL GeoFromWKB or others to create the geometry data.
		// Refer https://dev.mysql.com/doc/refman/5.7/en/gis-wkb-functions.html
		// I also find some go libs to handle WKB if possible
		// see https://github.com/twpayne/go-geom or https://github.com/paulmach/go.geo
		v, n, err = decodeBlob(data, meta)
	default:
		err = moerr.NewInternalErrorf(context.Background(), "unsupport type %d in binlog and don't know how to handle", tp)
	}

	return v, n, err
}

func (e *RowsEvent) decodeImage(data []byte, bitmap []byte, rowImageType byte) (int, error) {
	// Rows_log_event::print_verbose_one_row()

	pos := 0

	var isPartialJsonUpdate bool

	var partialBitmap []byte
	if e.eventType == PARTIAL_UPDATE_ROWS_EVENT && rowImageType == EnumRowImageTypeUpdateAI {
		binlogRowValueOptions, _, n := LengthEncodedInt(data[pos:]) // binlog_row_value_options
		pos += n
		isPartialJsonUpdate = byte(binlogRowValueOptions)&EnumBinlogRowValueOptionsPartialJsonUpdates != 0
		if isPartialJsonUpdate {
			byteCount := bitmapByteSize(int(e.Table.JsonColumnCount()))
			partialBitmap = data[pos : pos+byteCount]
			pos += byteCount
		}
	}

	row := make([]interface{}, e.ColumnCount)
	skips := make([]int, 0)

	// refer: https://github.com/alibaba/canal/blob/c3e38e50e269adafdd38a48c63a1740cde304c67/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L63
	count := 0
	for i := 0; i < int(e.ColumnCount); i++ {
		if isBitSet(bitmap, i) {
			count++
		}
	}
	count = bitmapByteSize(count)

	nullBitmap := data[pos : pos+count]
	pos += count

	partialBitmapIndex := 0
	nullBitmapIndex := 0

	for i := 0; i < int(e.ColumnCount); i++ {
		/*
		   Note: need to read partial bit before reading cols_bitmap, since
		   the partial_bits bitmap has a bit for every JSON column
		   regardless of whether it is included in the bitmap or not.
		*/
		isPartial := isPartialJsonUpdate &&
			(rowImageType == EnumRowImageTypeUpdateAI) &&
			(e.Table.ColumnType[i] == MYSQL_TYPE_JSON) &&
			isBitSetIncr(partialBitmap, &partialBitmapIndex)

		if !isBitSet(bitmap, i) {
			skips = append(skips, i)
			continue
		}

		if isBitSetIncr(nullBitmap, &nullBitmapIndex) {
			row[i] = nil
			continue
		}

		var n int
		var err error
		row[i], n, err = e.decodeValue(data[pos:], e.Table.ColumnType[i], e.Table.ColumnMeta[i], isPartial)

		if err != nil {
			return 0, err
		}
		pos += n
	}

	e.Rows = append(e.Rows, row)
	e.SkippedColumns = append(e.SkippedColumns, skips)
	return pos, nil
}

func (e *RowsEvent) DecodeData(pos int, data []byte) (err2 error) {
	// Rows_log_event::print_verbose()

	var (
		n   int
		err error
	)
	// ... repeat rows until event-end
	defer func() {
		if r := recover(); r != nil {
			err2 = moerr.NewInternalErrorf(context.Background(), "parse rows event panic %v, data %q, parsed rows %#v, table map %#v", r, data, e, e.Table)
		}
	}()

	// Pre-allocate memory for rows: before image + (optional) after image
	rowsLen := 1
	if e.needBitmap2 {
		rowsLen++
	}
	e.SkippedColumns = make([][]int, 0, rowsLen)
	e.Rows = make([][]interface{}, 0, rowsLen)

	var rowImageType byte
	switch e.eventType {
	case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2:
		rowImageType = EnumRowImageTypeWriteAI
	case DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
		rowImageType = EnumRowImageTypeDeleteBI
	default:
		rowImageType = EnumRowImageTypeUpdateBI
	}

	for pos < len(data) {
		// Parse the first image
		if n, err = e.decodeImage(data[pos:], e.ColumnBitmap1, rowImageType); err != nil {
			return err
		}
		pos += n

		// Parse the second image (for UPDATE only)
		if e.needBitmap2 {
			if n, err = e.decodeImage(data[pos:], e.ColumnBitmap2, EnumRowImageTypeUpdateAI); err != nil {
				return err
			}
			pos += n
		}
	}

	return nil
}

func (e *RowsEvent) Decode(data []byte) error {
	pos, err := e.DecodeHeader(data)
	if err != nil {
		return err
	}
	return e.DecodeData(pos, data)
}

func (e *TableMapEvent) bytesSlice2StrSlice(src [][]byte) []string {
	if src == nil {
		return nil
	}
	ret := make([]string, len(src))
	for i, item := range src {
		ret[i] = string(item)
	}
	return ret
}

// SetStrValueString returns values for set columns as string slices.
// nil is returned if not available or no set columns at all.
func (e *TableMapEvent) SetStrValueString() [][]string {
	if e.setStrValueString == nil {
		if len(e.SetStrValue) == 0 {
			return nil
		}
		e.setStrValueString = make([][]string, len(e.SetStrValue))
		for i, vals := range e.SetStrValue {
			e.setStrValueString[i] = e.bytesSlice2StrSlice(vals)
		}
	}
	return e.setStrValueString
}

// EnumStrValueString returns values for enum columns as string slices.
// nil is returned if not available or no enum columns at all.
func (e *TableMapEvent) EnumStrValueString() [][]string {
	if e.enumStrValueString == nil {
		if len(e.EnumStrValue) == 0 {
			return nil
		}
		e.enumStrValueString = make([][]string, len(e.EnumStrValue))
		for i, vals := range e.EnumStrValue {
			e.enumStrValueString[i] = e.bytesSlice2StrSlice(vals)
		}
	}
	return e.enumStrValueString
}

// ColumnNameString returns column names as string slice.
// nil is returned if not available.
func (e *TableMapEvent) ColumnNameString() []string {
	if e.columnNameString == nil {
		e.columnNameString = e.bytesSlice2StrSlice(e.ColumnName)
	}
	return e.columnNameString
}

// Below realType and IsXXXColumn are base from:
//   table_def::type in sql/rpl_utility.h
//   Table_map_log_event::print_columns in mysql-8.0/sql/log_event.cc and mariadb-10.5/sql/log_event_client.cc

func (e *TableMapEvent) realType(i int) byte {
	typ := e.ColumnType[i]

	switch typ {
	case MYSQL_TYPE_STRING:
		rtyp := byte(e.ColumnMeta[i] >> 8)
		if rtyp == MYSQL_TYPE_ENUM || rtyp == MYSQL_TYPE_SET {
			return rtyp
		}

	case MYSQL_TYPE_DATE:
		return MYSQL_TYPE_NEWDATE
	}

	return typ
}

func (e *TableMapEvent) IsNumericColumn(i int) bool {
	switch e.realType(i) {
	case MYSQL_TYPE_TINY,
		MYSQL_TYPE_SHORT,
		MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG,
		MYSQL_TYPE_LONGLONG,
		MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_FLOAT,
		MYSQL_TYPE_DOUBLE:
		return true

	default:
		return false
	}
}

// UnsignedMap returns a map: column index -> unsigned.
// Note that only numeric columns will be returned.
// nil is returned if not available or no numeric columns at all.
func (e *TableMapEvent) UnsignedMap() map[int]bool {
	if len(e.SignednessBitmap) == 0 {
		return nil
	}
	ret := make(map[int]bool)
	i := 0
	for _, field := range e.SignednessBitmap {
		for c := 0x80; c != 0; {
			if e.IsNumericColumn(i) {
				ret[i] = field&byte(c) != 0
				c >>= 1
			}
			i++
			if i >= int(e.ColumnCount) {
				return ret
			}
		}
	}
	return ret
}

func (e *TableMapEvent) collationMap(includeType func(int) bool, defaultCharset, columnCharset []uint64) map[int]uint64 {
	if len(defaultCharset) != 0 {
		defaultCollation := defaultCharset[0]

		// character column index -> collation
		collations := make(map[int]uint64)
		for i := 1; i < len(defaultCharset); i += 2 {
			collations[int(defaultCharset[i])] = defaultCharset[i+1]
		}

		p := 0
		ret := make(map[int]uint64)
		for i := 0; i < int(e.ColumnCount); i++ {
			if !includeType(i) {
				continue
			}

			if collation, ok := collations[p]; ok {
				ret[i] = collation
			} else {
				ret[i] = defaultCollation
			}
			p++
		}

		return ret
	}

	if len(columnCharset) != 0 {
		p := 0
		ret := make(map[int]uint64)
		for i := 0; i < int(e.ColumnCount); i++ {
			if !includeType(i) {
				continue
			}

			ret[i] = columnCharset[p]
			p++
		}

		return ret
	}

	return nil
}

// IsCharacterColumn returns true if the column type is considered as character type.
// Note that JSON/GEOMETRY types are treated as character type in mariadb.
// (JSON is an alias for LONGTEXT in mariadb: https://mariadb.com/kb/en/json-data-type/)
func (e *TableMapEvent) IsCharacterColumn(i int) bool {
	switch e.realType(i) {
	case MYSQL_TYPE_STRING,
		MYSQL_TYPE_VAR_STRING,
		MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_BLOB:
		return true

	case MYSQL_TYPE_GEOMETRY:
		return false

	default:
		return false
	}
}

// CollationMap returns a map: column index -> collation id.
// Note that only character columns will be returned.
// nil is returned if not available or no character columns at all.
func (e *TableMapEvent) CollationMap() map[int]uint64 {
	return e.collationMap(e.IsCharacterColumn, e.DefaultCharset, e.ColumnCharset)
}

func (e *TableMapEvent) IsEnumOrSetColumn(i int) bool {
	rtyp := e.realType(i)
	return rtyp == MYSQL_TYPE_ENUM || rtyp == MYSQL_TYPE_SET
}

// EnumSetCollationMap returns a map: column index -> collation id.
// Note that only enum or set columns will be returned.
// nil is returned if not available or no enum/set columns at all.
func (e *TableMapEvent) EnumSetCollationMap() map[int]uint64 {
	return e.collationMap(e.IsEnumOrSetColumn, e.EnumSetDefaultCharset, e.EnumSetColumnCharset)
}

func (e *TableMapEvent) strValueMap(includeType func(int) bool, strValue [][]string) map[int][]string {
	if len(strValue) == 0 {
		return nil
	}
	p := 0
	ret := make(map[int][]string)
	for i := 0; i < int(e.ColumnCount); i++ {
		if !includeType(i) {
			continue
		}
		ret[i] = strValue[p]
		p++
	}
	return ret
}

func (e *TableMapEvent) IsEnumColumn(i int) bool {
	return e.realType(i) == MYSQL_TYPE_ENUM
}

// EnumStrValueMap returns a map: column index -> enum string value.
// Note that only enum columns will be returned.
// nil is returned if not available or no enum columns at all.
func (e *TableMapEvent) EnumStrValueMap() map[int][]string {
	return e.strValueMap(e.IsEnumColumn, e.EnumStrValueString())
}

func (e *TableMapEvent) IsSetColumn(i int) bool {
	return e.realType(i) == MYSQL_TYPE_SET
}

// SetStrValueMap returns a map: column index -> set string value.
// Note that only set columns will be returned.
// nil is returned if not available or no set columns at all.
func (e *TableMapEvent) SetStrValueMap() map[int][]string {
	return e.strValueMap(e.IsSetColumn, e.SetStrValueString())
}

func (e *TableMapEvent) IsGeometryColumn(i int) bool {
	return e.realType(i) == MYSQL_TYPE_GEOMETRY
}

// GeometryTypeMap returns a map: column index -> geometry type.
// Note that only geometry columns will be returned.
// nil is returned if not available or no geometry columns at all.
func (e *TableMapEvent) GeometryTypeMap() map[int]uint64 {
	if len(e.GeometryType) == 0 {
		return nil
	}
	p := 0
	ret := make(map[int]uint64)
	for i := 0; i < int(e.ColumnCount); i++ {
		if !e.IsGeometryColumn(i) {
			continue
		}

		ret[i] = e.GeometryType[p]
		p++
	}
	return ret
}

// VisibilityMap returns a map: column index -> visiblity.
// Invisible column was introduced in MySQL 8.0.23
// nil is returned if not available.
func (e *TableMapEvent) VisibilityMap() map[int]bool {
	if len(e.VisibilityBitmap) == 0 {
		return nil
	}
	ret := make(map[int]bool)
	i := 0
	for _, field := range e.VisibilityBitmap {
		for c := 0x80; c != 0; c >>= 1 {
			ret[i] = field&byte(c) != 0
			i++
			if uint64(i) >= e.ColumnCount {
				return ret
			}
		}
	}
	return ret
}

// Nullable returns the nullablity of the i-th column.
// If null bits are not available, available is false.
// i must be in range [0, ColumnCount).
func (e *TableMapEvent) Nullable(i int) (available, nullable bool) {
	if len(e.NullBitmap) == 0 {
		return
	}
	return true, e.NullBitmap[i/8]&(1<<uint(i%8)) != 0
}

func (e *TableMapEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "TableID size: %d\n", e.tableIDSize)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Schema: %s\n", e.Schema)
	fmt.Fprintf(w, "Table: %s\n", e.Table)
	fmt.Fprintf(w, "Column count: %d\n", e.ColumnCount)
	fmt.Fprintf(w, "Column type: \n%s", hex.Dump(e.ColumnType))
	fmt.Fprintf(w, "NULL bitmap: \n%s", hex.Dump(e.NullBitmap))

	fmt.Fprintf(w, "Signedness bitmap: \n%s", hex.Dump(e.SignednessBitmap))
	fmt.Fprintf(w, "Default charset: %v\n", e.DefaultCharset)
	fmt.Fprintf(w, "Column charset: %v\n", e.ColumnCharset)
	fmt.Fprintf(w, "Set str value: %v\n", e.SetStrValueString())
	fmt.Fprintf(w, "Enum str value: %v\n", e.EnumStrValueString())
	fmt.Fprintf(w, "Column name: %v\n", e.ColumnNameString())
	fmt.Fprintf(w, "Geometry type: %v\n", e.GeometryType)
	fmt.Fprintf(w, "Primary key: %v\n", e.PrimaryKey)
	fmt.Fprintf(w, "Primary key prefix: %v\n", e.PrimaryKeyPrefix)
	fmt.Fprintf(w, "Enum/set default charset: %v\n", e.EnumSetDefaultCharset)
	fmt.Fprintf(w, "Enum/set column charset: %v\n", e.EnumSetColumnCharset)
	fmt.Fprintf(w, "Invisible Column bitmap: \n%s", hex.Dump(e.VisibilityBitmap))

	unsignedMap := e.UnsignedMap()
	fmt.Fprintf(w, "UnsignedMap: %#v\n", unsignedMap)

	collationMap := e.CollationMap()
	fmt.Fprintf(w, "CollationMap: %#v\n", collationMap)

	enumSetCollationMap := e.EnumSetCollationMap()
	fmt.Fprintf(w, "EnumSetCollationMap: %#v\n", enumSetCollationMap)

	enumStrValueMap := e.EnumStrValueMap()
	fmt.Fprintf(w, "EnumStrValueMap: %#v\n", enumStrValueMap)

	setStrValueMap := e.SetStrValueMap()
	fmt.Fprintf(w, "SetStrValueMap: %#v\n", setStrValueMap)

	geometryTypeMap := e.GeometryTypeMap()
	fmt.Fprintf(w, "GeometryTypeMap: %#v\n", geometryTypeMap)

	visibilityMap := e.VisibilityMap()
	fmt.Fprintf(w, "VisibilityMap: %#v\n", visibilityMap)

	nameMaxLen := 0
	for _, name := range e.ColumnName {
		if len(name) > nameMaxLen {
			nameMaxLen = len(name)
		}
	}
	nameFmt := "  %s"
	if nameMaxLen > 0 {
		nameFmt = fmt.Sprintf("  %%-%ds", nameMaxLen)
	}

	primaryKey := map[int]struct{}{}
	for _, pk := range e.PrimaryKey {
		primaryKey[int(pk)] = struct{}{}
	}

	fmt.Fprintf(w, "Columns: \n")
	for i := 0; i < int(e.ColumnCount); i++ {
		if len(e.ColumnName) == 0 {
			fmt.Fprintf(w, nameFmt, "<n/a>")
		} else {
			fmt.Fprintf(w, nameFmt, e.ColumnName[i])
		}

		fmt.Fprintf(w, "  type=%-3d", e.realType(i))

		if e.IsNumericColumn(i) {
			if len(unsignedMap) == 0 {
				fmt.Fprintf(w, "  unsigned=<n/a>")
			} else if unsignedMap[i] {
				fmt.Fprintf(w, "  unsigned=yes")
			} else {
				fmt.Fprintf(w, "  unsigned=no ")
			}
		}
		if e.IsCharacterColumn(i) {
			if len(collationMap) == 0 {
				fmt.Fprintf(w, "  collation=<n/a>")
			} else {
				fmt.Fprintf(w, "  collation=%d ", collationMap[i])
			}
		}
		if e.IsEnumColumn(i) {
			if len(enumSetCollationMap) == 0 {
				fmt.Fprintf(w, "  enum_collation=<n/a>")
			} else {
				fmt.Fprintf(w, "  enum_collation=%d", enumSetCollationMap[i])
			}

			if len(enumStrValueMap) == 0 {
				fmt.Fprintf(w, "  enum=<n/a>")
			} else {
				fmt.Fprintf(w, "  enum=%v", enumStrValueMap[i])
			}
		}
		if e.IsSetColumn(i) {
			if len(enumSetCollationMap) == 0 {
				fmt.Fprintf(w, "  set_collation=<n/a>")
			} else {
				fmt.Fprintf(w, "  set_collation=%d", enumSetCollationMap[i])
			}

			if len(setStrValueMap) == 0 {
				fmt.Fprintf(w, "  set=<n/a>")
			} else {
				fmt.Fprintf(w, "  set=%v", setStrValueMap[i])
			}
		}
		if e.IsGeometryColumn(i) {
			if len(geometryTypeMap) == 0 {
				fmt.Fprintf(w, "  geometry_type=<n/a>")
			} else {
				fmt.Fprintf(w, "  geometry_type=%v", geometryTypeMap[i])
			}
		}

		available, nullable := e.Nullable(i)
		if !available {
			fmt.Fprintf(w, "  null=<n/a>")
		} else if nullable {
			fmt.Fprintf(w, "  null=yes")
		} else {
			fmt.Fprintf(w, "  null=no ")
		}

		if _, ok := primaryKey[i]; ok {
			fmt.Fprintf(w, "  pri")
		}

		fmt.Fprintf(w, "\n")
	}

	fmt.Fprintln(w)
}

// FixedLengthInt: little endian
func FixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

func (e *TableMapEvent) decodeMeta(data []byte) error {
	pos := 0
	e.ColumnMeta = make([]uint16, e.ColumnCount)
	for i, t := range e.ColumnType {
		switch t {
		case MYSQL_TYPE_STRING:
			var x = uint16(data[pos]) << 8 // real type
			x += uint16(data[pos+1])       // pack or field length
			e.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_NEWDECIMAL:
			var x = uint16(data[pos]) << 8 // precision
			x += uint16(data[pos+1])       // decimals
			e.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT:
			e.ColumnMeta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case MYSQL_TYPE_BLOB,
			MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_GEOMETRY,
			MYSQL_TYPE_JSON:
			e.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_TIME2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP2:
			e.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB:
			return moerr.NewInternalErrorf(context.Background(), "unsupport type in binlog %d", t)
		default:
			e.ColumnMeta[i] = 0
		}
	}

	return nil
}

func bitmapByteSize(columnCount int) int {
	return (columnCount + 7) / 8
}

func (e *TableMapEvent) decodeIntSeq(v []byte) (ret []uint64, err error) {
	p := 0
	for p < len(v) {
		i, _, n := LengthEncodedInt(v[p:])
		p += n
		ret = append(ret, i)
	}
	return
}

func (e *TableMapEvent) decodeDefaultCharset(v []byte) (ret []uint64, err error) {
	ret, err = e.decodeIntSeq(v)
	if err != nil {
		return
	}
	if len(ret)%2 != 1 {
		return nil, moerr.NewInternalErrorf(context.Background(), "Expect odd item in DefaultCharset but got %d", len(ret))
	}
	return
}

func (e *TableMapEvent) decodeColumnNames(v []byte) error {
	p := 0
	e.ColumnName = make([][]byte, 0, e.ColumnCount)
	for p < len(v) {
		n := int(v[p])
		p++
		e.ColumnName = append(e.ColumnName, v[p:p+n])
		p += n
	}

	if len(e.ColumnName) != int(e.ColumnCount) {
		return moerr.NewInternalErrorf(context.Background(), "Expect %d column names but got %d", e.ColumnCount, len(e.ColumnName))
	}
	return nil
}

func (e *TableMapEvent) decodeStrValue(v []byte) (ret [][][]byte, err error) {
	p := 0
	for p < len(v) {
		nVal, _, n := LengthEncodedInt(v[p:])
		p += n
		vals := make([][]byte, 0, int(nVal))
		for i := 0; i < int(nVal); i++ {
			val, _, n, err := LengthEncodedString(v[p:])
			if err != nil {
				return nil, err
			}
			p += n
			vals = append(vals, val)
		}
		ret = append(ret, vals)
	}
	return
}

func (e *TableMapEvent) decodeSimplePrimaryKey(v []byte) error {
	p := 0
	for p < len(v) {
		i, _, n := LengthEncodedInt(v[p:])
		e.PrimaryKey = append(e.PrimaryKey, i)
		e.PrimaryKeyPrefix = append(e.PrimaryKeyPrefix, 0)
		p += n
	}
	return nil
}

func (e *TableMapEvent) decodePrimaryKeyWithPrefix(v []byte) error {
	p := 0
	for p < len(v) {
		i, _, n := LengthEncodedInt(v[p:])
		e.PrimaryKey = append(e.PrimaryKey, i)
		p += n
		i, _, n = LengthEncodedInt(v[p:])
		e.PrimaryKeyPrefix = append(e.PrimaryKeyPrefix, i)
		p += n
	}
	return nil
}

func (e *TableMapEvent) decodeOptionalMeta(data []byte) (err error) {
	pos := 0
	for pos < len(data) {
		// optional metadata fields are stored in Type, Length, Value(TLV) format
		// Type takes 1 byte. Length is a packed integer value. Values takes Length bytes
		t := data[pos]
		pos++

		l, _, n := LengthEncodedInt(data[pos:])
		pos += n

		v := data[pos : pos+int(l)]
		pos += int(l)

		switch t {
		case TABLE_MAP_OPT_META_SIGNEDNESS:
			e.SignednessBitmap = v

		case TABLE_MAP_OPT_META_DEFAULT_CHARSET:
			e.DefaultCharset, err = e.decodeDefaultCharset(v)
			if err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_COLUMN_CHARSET:
			e.ColumnCharset, err = e.decodeIntSeq(v)
			if err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_COLUMN_NAME:
			if err = e.decodeColumnNames(v); err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_SET_STR_VALUE:
			e.SetStrValue, err = e.decodeStrValue(v)
			if err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_ENUM_STR_VALUE:
			e.EnumStrValue, err = e.decodeStrValue(v)
			if err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_GEOMETRY_TYPE:
			e.GeometryType, err = e.decodeIntSeq(v)
			if err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY:
			if err = e.decodeSimplePrimaryKey(v); err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_PRIMARY_KEY_WITH_PREFIX:
			if err = e.decodePrimaryKeyWithPrefix(v); err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_ENUM_AND_SET_DEFAULT_CHARSET:
			e.EnumSetDefaultCharset, err = e.decodeDefaultCharset(v)
			if err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_ENUM_AND_SET_COLUMN_CHARSET:
			e.EnumSetColumnCharset, err = e.decodeIntSeq(v)
			if err != nil {
				return err
			}

		case TABLE_MAP_OPT_META_COLUMN_VISIBILITY:
			e.VisibilityBitmap = v

		default:
			// Ignore for future extension
		}
	}

	return nil
}

func (e *TableMapEvent) Decode(data []byte) error {
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.tableIDSize])
	pos += e.tableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	schemaLength := data[pos]
	pos++

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	// skip 0x00
	pos++

	tableLength := data[pos]
	pos++

	e.Table = data[pos : pos+int(tableLength)]
	pos += int(tableLength)

	// skip 0x00
	pos++

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	e.ColumnType = data[pos : pos+int(e.ColumnCount)]
	pos += int(e.ColumnCount)

	var err error
	var metaData []byte
	if metaData, _, n, err = LengthEncodedString(data[pos:]); err != nil {
		return err
	}

	if err = e.decodeMeta(metaData); err != nil {
		return err
	}

	pos += n

	nullBitmapSize := bitmapByteSize(int(e.ColumnCount))
	if len(data[pos:]) < nullBitmapSize {
		return io.EOF
	}

	e.NullBitmap = data[pos : pos+nullBitmapSize]

	pos += nullBitmapSize

	if e.optionalMetaDecodeFunc != nil {
		if err = e.optionalMetaDecodeFunc(data[pos:]); err != nil {
			return err
		}
	} else {
		if err = e.decodeOptionalMeta(data[pos:]); err != nil {
			return err
		}
	}

	return nil
}

func (e *XIDEvent) Decode(data []byte) error {
	e.XID = binary.LittleEndian.Uint64(data)
	return nil
}

func (e *XIDEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "XID: %d\n", e.XID)
	if e.GSet != nil {
		fmt.Fprintf(w, "GTIDSet: %s\n", e.GSet.String())
	}
	fmt.Fprintln(w)
}

func (e *QueryEvent) Decode(data []byte) error {
	pos := 0

	e.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	schemaLength := data[pos]
	pos++

	e.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	statusVarsLength := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.StatusVars = data[pos : pos+int(statusVarsLength)]
	pos += int(statusVarsLength)

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	e.Query = data[pos:]

	return nil
}

func (e *QueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Slave proxy ID: %d\n", e.SlaveProxyID)
	fmt.Fprintf(w, "Execution time: %d\n", e.ExecutionTime)
	fmt.Fprintf(w, "Error code: %d\n", e.ErrorCode)
	//fmt.Fprintf(w, "Status vars: \n%s", hex.Dump(e.StatusVars))
	fmt.Fprintf(w, "Schema: %s\n", e.Schema)
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	if e.GSet != nil {
		fmt.Fprintf(w, "GTIDSet: %s\n", e.GSet.String())
	}
	fmt.Fprintln(w)
}

func (e *RotateEvent) Decode(data []byte) error {
	e.Position = binary.LittleEndian.Uint64(data[0:])
	e.NextLogName = data[8:]

	return nil
}

func (e *RotateEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Position: %d\n", e.Position)
	fmt.Fprintf(w, "Next log name: %s\n", e.NextLogName)
	fmt.Fprintln(w)
}

// server version format X.Y.Zabc, a is not . or number
func splitServerVersion(server string) []int {
	seps := strings.Split(server, ".")
	if len(seps) < 3 {
		return []int{0, 0, 0}
	}

	x, _ := strconv.Atoi(seps[0])
	y, _ := strconv.Atoi(seps[1])

	index := 0
	for i, c := range seps[2] {
		if !unicode.IsNumber(c) {
			index = i
			break
		}
	}

	z, _ := strconv.Atoi(seps[2][0:index])

	return []int{x, y, z}
}

func calcVersionProduct(server string) int {
	versionSplit := splitServerVersion(server)

	return (versionSplit[0]*256+versionSplit[1])*256 + versionSplit[2]
}

func (e *FormatDescriptionEvent) Decode(data []byte) error {
	pos := 0
	e.Version = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	serverVersionRaw := make([]byte, 50)
	copy(serverVersionRaw, data[pos:])
	pos += 50

	e.CreateTimestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.EventHeaderLength = data[pos]
	pos++

	if e.EventHeaderLength != byte(EventHeaderSize) {
		return moerr.NewInternalErrorf(context.Background(), "invalid event header length %d, must 19", e.EventHeaderLength)
	}

	serverVersionLength := bytes.Index(serverVersionRaw, []byte{0x0})
	if serverVersionLength < 0 {
		e.ServerVersion = string(serverVersionRaw)
	} else {
		e.ServerVersion = string(serverVersionRaw[:serverVersionLength])
	}
	checksumProduct := checksumVersionProductMysql

	if calcVersionProduct(e.ServerVersion) >= checksumProduct {
		// here, the last 5 bytes is 1 byte check sum alg type and 4 byte checksum if exists
		e.ChecksumAlgorithm = data[len(data)-5]
		e.EventTypeHeaderLengths = data[pos : len(data)-5]
	} else {
		e.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
		e.EventTypeHeaderLengths = data[pos:]
	}

	return nil
}

func (e *FormatDescriptionEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Version: %d\n", e.Version)
	fmt.Fprintf(w, "Server version: %s\n", e.ServerVersion)
	//fmt.Fprintf(w, "Create date: %s\n", time.Unix(int64(e.CreateTimestamp), 0).Format(TimeFormat))
	fmt.Fprintf(w, "Checksum algorithm: %d\n", e.ChecksumAlgorithm)
	//fmt.Fprintf(w, "Event header lengths: \n%s", hex.Dump(e.EventTypeHeaderLengths))
	fmt.Fprintln(w)
}

func (i Interval) String() string {
	if i.Stop == i.Start+1 {
		return fmt.Sprintf("%d", i.Start)
	} else {
		return fmt.Sprintf("%d-%d", i.Start, i.Stop-1)
	}
}

func (s *UUIDSet) Bytes() []byte {
	var buf bytes.Buffer

	buf.WriteString(s.SID.String())

	for _, i := range s.Intervals {
		buf.WriteString(":")
		buf.WriteString(i.String())
	}

	return buf.Bytes()
}

func (s *UUIDSet) String() string {
	return hack.String(s.Bytes())
}

func (s GTIDSet) String() string {
	// there is only one element in gtid set
	if len(s) == 1 {
		for _, set := range s {
			return set.String()
		}
	}

	// sort multi set
	var buf bytes.Buffer
	sets := make([]string, 0, len(s))
	for _, set := range s {
		sets = append(sets, set.String())
	}
	sort.Strings(sets)

	sep := ""
	for _, set := range sets {
		buf.WriteString(sep)
		buf.WriteString(set)
		sep = ","
	}

	return hack.String(buf.Bytes())
}

func (s IntervalSlice) Len() int {
	return len(s)
}

func (s IntervalSlice) Less(i, j int) bool {
	if s[i].Start < s[j].Start {
		return true
	} else if s[i].Start > s[j].Start {
		return false
	} else {
		return s[i].Stop < s[j].Stop
	}
}

func (s IntervalSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (c *RConn) SetAttributes(attributes map[string]string) {
	for k, v := range attributes {
		c.attributes[k] = v
	}
}

func NewBinlogSyncer(ctx context.Context, cfg BinlogSyncerConfig) *BinlogSyncer {
	if cfg.ServerID == 0 {
		logutil.Error("can't use 0 as the server ID")
	}
	if cfg.EventCacheCount == 0 {
		cfg.EventCacheCount = 10240
	}

	// Clear the Password to avoid outputting it in logs.
	pass := cfg.Password
	cfg.Password = ""
	logutil.Infof("create BinlogSyncer with config %+v", cfg)
	cfg.Password = pass

	b := new(BinlogSyncer)

	b.cfg = cfg

	b.parser = NewBinlogParser()
	b.parser.rawMode = false
	b.parser.parseTime = false
	b.running = false
	b.ctx, b.cancel = context.WithCancel(ctx)

	return b
}

// Interval is [start, stop), but the GTID string's format is [n] or [n1-n2], closed interval
func parseInterval(str string) (i Interval, err error) {
	p := strings.Split(str, "-")
	switch len(p) {
	case 1:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		i.Stop = i.Start + 1
	case 2:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		if err == nil {
			i.Stop, err = strconv.ParseInt(p[1], 10, 64)
			i.Stop++
		}
	default:
		err = moerr.NewInternalError(context.Background(), "invalid interval format, must n[-n]")
	}

	if err != nil {
		return
	}

	if i.Stop <= i.Start {
		err = moerr.NewInternalError(context.Background(), "invalid interval format, must start or stop")
	}

	return
}

func ParseUUIDSet(str string) (*UUIDSet, error) {
	str = strings.TrimSpace(str)
	sep := strings.Split(str, ":")
	if len(sep) < 2 {
		return nil, moerr.NewInternalError(context.Background(), "invalid GTID format, must UUID:interval[:interval]")
	}

	var err error
	s := new(UUIDSet)
	if s.SID, err = uuid.Parse(sep[0]); err != nil {
		return nil, err
	}

	// Handle interval
	for i := 1; i < len(sep); i++ {
		if in, err := parseInterval(sep[i]); err != nil {
			return nil, err
		} else {
			s.Intervals = append(s.Intervals, in)
		}
	}

	s.Intervals = s.Intervals.Normalize()

	return s, nil
}

func (s IntervalSlice) Sort() {
	sort.Sort(s)
}

func (s IntervalSlice) Normalize() IntervalSlice {
	var n IntervalSlice
	if len(s) == 0 {
		return n
	}

	s.Sort()

	n = append(n, s[0])

	for i := 1; i < len(s); i++ {
		last := n[len(n)-1]
		if s[i].Start > last.Stop {
			n = append(n, s[i])
			continue
		} else {
			stop := s[i].Stop
			if last.Stop > stop {
				stop = last.Stop
			}
			n[len(n)-1] = Interval{last.Start, stop}
		}
	}

	return n
}

func (s *UUIDSet) AddInterval(in IntervalSlice) {
	s.Intervals = append(s.Intervals, in...)
	s.Intervals = s.Intervals.Normalize()
}

func (s *GTIDSet) AddSet(set *UUIDSet) {
	if set == nil {
		return
	}
	sid := set.SID.String()
	o, ok := (*s)[sid]
	if ok {
		o.AddInterval(set.Intervals)
	} else {
		(*s)[sid] = set
	}
}

func ParseGTIDSet(str string) (GTIDSet, error) {
	s := make(GTIDSet)
	if str == "" {
		return s, nil
	}

	sp := strings.Split(str, ",")

	//todo, handle redundant same uuid
	for i := 0; i < len(sp); i++ {
		if set, err := ParseUUIDSet(sp[i]); err != nil {
			return nil, err
		} else {
			s.AddSet(set)
		}
	}
	return s, nil
}

func Parse(r io.Reader) (GTIDSet, error) {
	rb := bufio.NewReaderSize(r, 1024*16)
	gtid := GTIDSet{}
	for {
		line, err := rb.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		// Ignore '\n' on Linux or '\r\n' on Windows
		line = strings.TrimRightFunc(line, func(c rune) bool {
			return c == '\r' || c == '\n'
		})

		// parsed gtid set from mysqldump
		// gtid comes before binlog file-position
		if m := gtidExp.FindAllStringSubmatch(line, -1); len(m) == 1 {
			gtidStr := m[0][1]
			if gtidStr != "" {
				gtid, err = ParseGTIDSet(gtidStr)
				if err != nil {
					return nil, err
				} else {
					break
				}
			}
		}
	}

	return gtid, nil
}

func Dump(w io.Writer) error {
	args := make([]string, 0, 4)

	host := "127.0.0.1"
	port := "3306"
	user := "snan"
	password := "19990928"

	args = append(args, fmt.Sprintf("--host=%s", host))
	args = append(args, fmt.Sprintf("--port=%s", port))
	args = append(args, fmt.Sprintf("--user=%s", user))
	args = append(args, fmt.Sprintf("--password=%s", password))
	args = append(args, "test test_c")

	cmd := exec.Command("/usr/bin/mysqldump", args...)
	cmd.Stdout = w
	return cmd.Run()
}

func GetGTIDSet() (GTIDSet, error) {
	r, w := io.Pipe()

	go func() {
		err := Dump(w)
		_ = w.CloseWithError(err)
	}()

	gtid, err := Parse(r)
	_ = r.CloseWithError(err)

	return gtid, err
}

func (b *BinlogSyncer) isClosed() bool {
	select {
	case <-b.ctx.Done():
		return true
	default:
		return false
	}
}

func BytesBufferGet() (data *bytes.Buffer) {
	data = bytesBufferPool.Get().(*bytes.Buffer)
	data.Reset()
	return data
}

func BytesBufferPut(data *bytes.Buffer) {
	if data == nil || data.Len() > TooBigBlockSize {
		return
	}
	bytesBufferPool.Put(data)
}

func (c *RConn) copyN(dst io.Writer, n int64) (int64, error) {
	var written int64

	for n > 0 {
		bcap := cap(c.copyNBuf)
		if int64(bcap) > n {
			bcap = int(n)
		}
		buf := c.copyNBuf[:bcap]

		// Call ReadAtLeast with the currentPacketReader as it may change on every iteration
		// of this loop.
		rd, err := io.ReadAtLeast(c.reader, buf, bcap)

		n -= int64(rd)

		// ReadAtLeast will return EOF or ErrUnexpectedEOF when fewer than the min
		// bytes are read. In this case, and when we have compression then advance
		// the sequence number and reset the compressed reader to continue reading
		// the remaining bytes in the next compressed packet.

		if err != nil {
			return written, err
		}

		// careful to only write from the buffer the number of bytes read
		wr, err := dst.Write(buf[:rd])
		written += int64(wr)
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func (c *RConn) ReadPacketTo(w io.Writer) error {
	b := BytesBufferGet()
	defer func() {
		BytesBufferPut(b)
	}()
	// packets that come in a compressed packet may be partial
	// so use the copyN function to read the packet header into a
	// buffer, since copyN is capable of getting the next compressed
	// packet and updating the Conn state with a new compressedReader.
	if _, err := c.copyN(b, 4); err != nil {
		return moerr.NewInternalErrorf(context.Background(), "connection was bad io.ReadFull(header) failed. err %v", err)
	} else {
		// copy was successful so copy the 4 bytes from the buffer to the header
		copy(c.header[:4], b.Bytes()[:4])
	}

	length := int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
	sequence := c.header[3]

	if sequence != c.Sequence {
		return moerr.NewInternalErrorf(context.Background(), "invalid sequence %d != %d", sequence, c.Sequence)
	}

	c.Sequence++

	if buf, ok := w.(*bytes.Buffer); ok {
		// Allocate the buffer with expected length directly instead of call `grow` and migrate data many times.
		buf.Grow(length)
	}

	if n, err := c.copyN(w, int64(length)); err != nil {
		return moerr.NewInternalErrorf(context.Background(), "connection was bad io.CopyN failed. err %v, copied %v, expected %v", err, n, length)
	} else if n != int64(length) {
		return moerr.NewInternalErrorf(context.Background(), "connection was bad io.CopyN failed(n != int64(length)). %v bytes copied, while %v expected", n, length)
	} else {
		if length < MaxPayloadLen {
			return nil
		}

		if err = c.ReadPacketTo(w); err != nil {
			return moerr.NewInternalErrorf(context.Background(), "ReadPacketTo failed %v", err)
		}
	}

	return nil
}

func (c *RConn) ReadPacketReuseMem(dst []byte) ([]byte, error) {
	// Here we use `sync.Pool` to avoid allocate/destroy buffers frequently.
	buf := BytesBufferGet()
	defer func() {
		BytesBufferPut(buf)
	}()

	if err := c.ReadPacketTo(buf); err != nil {
		return nil, err
	}

	readBytes := buf.Bytes()
	readSize := len(readBytes)
	var result []byte
	if len(dst) > 0 {
		result = append(dst, readBytes...)
		// if read block is big, do not cache buf anymore
		if readSize > TooBigBlockSize {
			buf = nil
		}
	} else {
		if readSize > TooBigBlockSize {
			// if read block is big, use read block as result and do not cache buf anymore
			result = readBytes
			buf = nil
		} else {
			result = append(dst, readBytes...)
		}
	}

	return result, nil
}

func (c *RConn) readInitialHandshake() error {
	data, err := c.ReadPacketReuseMem(nil)
	if err != nil {
		return err
	}

	if data[0] == ERR_HEADER {
		return moerr.NewInternalError(context.Background(), "read initial handshake error")
	}

	if data[0] != ClassicProtocolVersion {
		if data[0] == XProtocolVersion {
			return moerr.NewInternalErrorf(context.Background(), "invalid protocol version %d, expected 10. "+
				"This might be X Protocol, make sure to connect to the right port",
				data[0])
		}
		return moerr.NewInternalErrorf(context.Background(), "invalid protocol version %d, expected 10", data[0])
	}
	pos := 1

	// skip mysql version
	// mysql version end with 0x00
	version := data[pos : bytes.IndexByte(data[pos:], 0x00)+1]
	c.serverVersion = string(version)
	pos += len(version) + 1 /*trailing zero byte*/

	// connection id length is 4
	c.connectionID = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	// first 8 bytes of the plugin provided data (scramble)
	c.salt = append(c.salt[:0], data[pos:pos+8]...)
	pos += 8

	if data[pos] != 0 { // 	0x00 byte, terminating the first part of a scramble
		return moerr.NewInternalErrorf(context.Background(), "expect 0x00 after scramble, got %q", rune(data[pos]))
	}
	pos++

	// The lower 2 bytes of the Capabilities Flags
	c.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))
	// check protocol
	if c.capability&CLIENT_PROTOCOL_41 == 0 {
		return moerr.NewInternalError(context.Background(), "the MySQL server can not support protocol 41 and above required by the client")
	}
	pos += 2

	if len(data) > pos {
		// default server a_protocol_character_set, only the lower 8-bits
		// c.charset = data[pos]
		pos += 1

		c.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		// The upper 2 bytes of the Capabilities Flags
		c.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | c.capability
		pos += 2

		// length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
		authPluginDataLen := data[pos]
		if (c.capability&CLIENT_PLUGIN_AUTH == 0) && (authPluginDataLen > 0) {
			return moerr.NewInternalErrorf(context.Background(), "invalid auth plugin data filler %d", authPluginDataLen)
		}
		pos++

		// skip reserved (all [00] ?)
		pos += 10

		if c.capability&CLIENT_SECURE_CONNECTION != 0 {
			// Rest of the plugin provided data (scramble)

			// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
			// $len=MAX(13, length of auth-plugin-data - 8)
			//
			// https://github.com/mysql/mysql-server/blob/1bfe02bdad6604d54913c62614bde57a055c8332/sql/auth/sql_authentication.cc#L1641-L1642
			// the first packet *must* have at least 20 bytes of a scramble.
			// if a plugin provided less, we pad it to 20 with zeros
			rest := int(authPluginDataLen) - 8
			if rest < 13 {
				rest = 13
			}

			authPluginDataPart2 := data[pos : pos+rest-1]
			pos += rest

			c.salt = append(c.salt, authPluginDataPart2...)
		}

		if c.capability&CLIENT_PLUGIN_AUTH != 0 {
			c.authPluginName = string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
			pos += len(c.authPluginName)

			if data[pos] != 0 {
				return moerr.NewInternalErrorf(context.Background(), "expect 0x00 after authPluginName, got %q", rune(data[pos]))
			}
			// pos++ // ineffectual
		}
	}

	// if server gives no default auth plugin name, use a client default
	if c.authPluginName == "" {
		c.authPluginName = defaultAuthPluginName
	}

	return nil
}

// helper function to determine what auth methods are allowed by this client
func authPluginAllowed(pluginName string) bool {
	for _, p := range supportedAuthPlugins {
		if pluginName == p {
			return true
		}
	}
	return false
}

func CalcPassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// CalcCachingSha2Password: Hash password using MySQL 8+ method (SHA256)
func CalcCachingSha2Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

// generate auth response data according to auth plugin
//
// NOTE: the returned boolean value indicates whether to add a \NUL to the end of data.
// it is quite tricky because MySQL server expects different formats of responses in different auth situations.
// here the \NUL needs to be added when sending back the empty password or cleartext password in 'sha256_password'
// authentication.
func (c *RConn) genAuthResponse(authData []byte) ([]byte, bool, error) {
	// password hashing
	switch c.authPluginName {
	case AUTH_NATIVE_PASSWORD:
		return CalcPassword(authData[:20], []byte(c.password)), false, nil
	case AUTH_CACHING_SHA2_PASSWORD:
		return CalcCachingSha2Password(authData, c.password), false, nil
	case AUTH_CLEAR_PASSWORD:
		return []byte(c.password), true, nil
	case AUTH_SHA256_PASSWORD:
		if len(c.password) == 0 {
			return nil, true, nil
		}

		// request public key from server
		// see: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
		return []byte{1}, false, nil
	default:
		// not reachable
		return nil, false, moerr.NewInternalErrorf(context.Background(), "auth plugin '%s' is not supported", c.authPluginName)
	}
}

// AppendLengthEncodedInteger: encodes a uint64 value and appends it to the given bytes slice
func AppendLengthEncodedInteger(b []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(b, byte(n))

	case n <= 0xffff:
		return append(b, 0xfc, byte(n), byte(n>>8))

	case n <= 0xffffff:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	}
	return append(b, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
		byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
}

func PutLengthEncodedInt(n uint64) []byte {
	switch {
	case n <= 250:
		return []byte{byte(n)}

	case n <= 0xffff:
		return []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

	default:
		// handles case n <= 0xffffffffffffffff
		// using 'default' instead of 'case' to avoid static analysis error
		// SA4003: every value of type uint64 is <= math.MaxUint64
		return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
	}
}

func PutLengthEncodedString(b []byte) []byte {
	data := make([]byte, 0, len(b)+9)
	data = append(data, PutLengthEncodedInt(uint64(len(b)))...)
	data = append(data, b...)
	return data
}

// generate connection attributes data
func (c *RConn) genAttributes() []byte {
	if len(c.attributes) == 0 {
		return nil
	}

	attrData := make([]byte, 0)
	for k, v := range c.attributes {
		attrData = append(attrData, PutLengthEncodedString([]byte(k))...)
		attrData = append(attrData, PutLengthEncodedString([]byte(v))...)
	}
	return append(PutLengthEncodedInt(uint64(len(attrData))), attrData...)
}

// WritePacket data already has 4 bytes header will modify data in-place
func (c *RConn) WritePacket(data []byte) error {
	length := len(data) - 4

	for length >= MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = c.Sequence

		if n, err := c.Write(data[:4+MaxPayloadLen]); err != nil {
			return moerr.NewInternalErrorf(context.Background(), "connection was bad. Write(payload portion) failed. err %v", err)
		} else if n != (4 + MaxPayloadLen) {
			return moerr.NewInternalErrorf(context.Background(), "connection was bad. Write(payload portion) failed. only %v bytes written, while %v expected", n, 4+MaxPayloadLen)
		} else {
			c.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = c.Sequence

	if n, err := c.Write(data); err != nil {
		return moerr.NewInternalErrorf(context.Background(), "connection was bad. Write failed. err %v", err)
	} else if n != len(data) {
		return moerr.NewInternalErrorf(context.Background(), "connection was bad. Write failed. only %v bytes written, while %v expected", n, len(data))
	}

	c.Sequence++
	return nil
}

// See: http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (c *RConn) writeAuthHandshake() error {
	if !authPluginAllowed(c.authPluginName) {
		return moerr.NewInternalErrorf(context.Background(), "unknow auth plugin name '%s'", c.authPluginName)
	}

	// Set default client capabilities that reflect the abilities of this library
	capability := CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_PLUGIN_AUTH
	// Adjust client capability flags based on server support
	capability |= c.capability & CLIENT_LONG_FLAG
	// Adjust client capability flags on specific client requests
	// Only flags that would make any sense setting and aren't handled elsewhere
	// in the library are supported here
	capability |= c.ccaps&CLIENT_FOUND_ROWS | c.ccaps&CLIENT_IGNORE_SPACE |
		c.ccaps&CLIENT_MULTI_STATEMENTS | c.ccaps&CLIENT_MULTI_RESULTS |
		c.ccaps&CLIENT_PS_MULTI_RESULTS | c.ccaps&CLIENT_CONNECT_ATTRS |
		c.ccaps&CLIENT_COMPRESS | c.ccaps&CLIENT_ZSTD_COMPRESSION_ALGORITHM |
		c.ccaps&CLIENT_LOCAL_FILES

	auth, addNull, err := c.genAuthResponse(c.salt)
	if err != nil {
		return err
	}

	// encode length of the auth plugin data
	// here we use the Length-Encoded-Integer(LEI) as the data length may not fit into one byte
	// see: https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
	var authRespLEIBuf [9]byte
	authRespLEI := AppendLengthEncodedInteger(authRespLEIBuf[:0], uint64(len(auth)))
	if len(authRespLEI) > 1 {
		// if the length can not be written in 1 byte, it must be written as a
		// length encoded integer
		capability |= CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	}

	// packet length
	// capability 4
	// max-packet size 4
	// charset 1
	// reserved all[0] 23
	// username
	// auth
	// mysql_native_password + null-terminated
	length := 4 + 4 + 1 + 23 + len(c.user) + 1 + len(authRespLEI) + len(auth) + 21 + 1
	if addNull {
		length++
	}
	// db name
	if len(c.db) > 0 {
		capability |= CLIENT_CONNECT_WITH_DB
		length += len(c.db) + 1
	}
	// connection attributes
	attrData := c.genAttributes()
	if len(attrData) > 0 {
		capability |= CLIENT_CONNECT_ATTRS
		length += len(attrData)
	}
	if c.ccaps&CLIENT_ZSTD_COMPRESSION_ALGORITHM > 0 {
		length++
	}

	data := make([]byte, length+4)

	// capability [32 bit]
	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	// MaxPacketSize [32 bit] (none)
	data[8] = 0x00
	data[9] = 0x00
	data[10] = 0x00
	data[11] = 0x00

	// Charset [1 byte]
	// use default collation id 33 here, is `utf8mb3_general_ci`
	collationName := c.collation
	if len(collationName) == 0 {
		collationName = DEFAULT_COLLATION_NAME
	}
	collation, err := charset.GetCollationByName(collationName)
	if err != nil {
		return moerr.NewInternalErrorf(context.Background(), "invalid collation name %s", collationName)
	}

	// the MySQL protocol calls for the collation id to be sent as 1 byte, where only the
	// lower 8 bits are used in this field.
	data[12] = byte(collation.ID & 0xff)

	// Filler [23 bytes] (all 0x00)
	pos := 13
	for ; pos < 13+23; pos++ {
		data[pos] = 0
	}

	// User [null terminated string]
	if len(c.user) > 0 {
		pos += copy(data[pos:], c.user)
	}
	data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	pos += copy(data[pos:], authRespLEI)
	pos += copy(data[pos:], auth)
	if addNull {
		data[pos] = 0x00
		pos++
	}

	// db [null terminated string]
	if len(c.db) > 0 {
		pos += copy(data[pos:], c.db)
		data[pos] = 0x00
		pos++
	}

	// Assume native client during response
	pos += copy(data[pos:], c.authPluginName)
	data[pos] = 0x00
	pos++

	// connection attributes
	if len(attrData) > 0 {
		pos += copy(data[pos:], attrData)
	}

	if c.ccaps&CLIENT_ZSTD_COMPRESSION_ALGORITHM > 0 {
		// zstd_compression_level
		data[pos] = 0x03
	}

	return c.WritePacket(data)
}

func LengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	if len(b) == 0 {
		return 0, true, 0
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

		// 252: value of following 2
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, false, 3

		// 253: value of following 3
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, false, 4

		// 254: value of following 8
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
				uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
				uint64(b[7])<<48 | uint64(b[8])<<56,
			false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}

func (c *RConn) handleOKPacket(data []byte) (*Result, error) {
	var n int
	var pos = 1

	r := new(Result)

	r.AffectedRows, _, n = LengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = LengthEncodedInt(data[pos:])
	pos += n

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		pos += 2

		//todo:strict_mode, check warnings as error
		r.Warnings = binary.LittleEndian.Uint16(data[pos:])
		// pos += 2
	} else if c.capability&CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		// pos += 2
	}

	// new ok package will check CLIENT_SESSION_TRACK too, but I don't support it now.

	// skip info
	return r, nil
}

func (c *RConn) readAuthResult() ([]byte, string, error) {
	data, err := c.ReadPacketReuseMem(nil)
	if err != nil {
		return nil, "", moerr.NewInternalErrorf(context.Background(), "ReadPacket: %w", err)
	}

	// see: https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	// packet indicator
	switch data[0] {
	case OK_HEADER:
		_, err := c.handleOKPacket(data)
		return nil, "", err

	case MORE_DATE_HEADER:
		return data[1:], "", err

	case EOF_HEADER:
		// server wants to switch auth
		if len(data) < 1 {
			// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::OldAuthSwitchRequest
			return nil, AUTH_MYSQL_OLD_PASSWORD, nil
		}
		pluginEndIndex := bytes.IndexByte(data, 0x00)
		if pluginEndIndex < 0 {
			return nil, "", moerr.NewInternalError(context.Background(), "invalid packet")
		}
		plugin := string(data[1:pluginEndIndex])
		authData := data[pluginEndIndex+1:]
		return authData, plugin, nil

	default: // Error otherwise
		return nil, "", moerr.NewInternalError(context.Background(), "invalid packet")
	}
}

// WriteAuthSwitchPacket see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (c *RConn) WriteAuthSwitchPacket(authData []byte, addNUL bool) error {
	pktLen := 4 + len(authData)
	if addNUL {
		pktLen++
	}
	data := make([]byte, pktLen)

	// Add the auth data [EOF]
	copy(data[4:], authData)
	if addNUL {
		data[pktLen-1] = 0x00
	}

	return moerr.NewInternalErrorf(context.Background(), "WritePacket failed, %v", c.WritePacket(data))
}

func (c *RConn) readOK() (*Result, error) {
	data, err := c.ReadPacketReuseMem(nil)
	if err != nil {
		return nil, err
	}

	if data[0] == OK_HEADER {
		return c.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return nil, moerr.NewInternalError(context.Background(), "invalid packet")
	} else {
		return nil, moerr.NewInternalError(context.Background(), "invalid ok packet")
	}
}

// WritePublicKeyAuthPacket Caching sha2 authentication. Public key request and send encrypted password
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (c *RConn) WritePublicKeyAuthPacket(password string, cipher []byte) error {
	// request public key
	data := make([]byte, 4+1)
	data[4] = 2 // cachingSha2PasswordRequestPublicKey
	if err := c.WritePacket(data); err != nil {
		return moerr.NewInternalErrorf(context.Background(), "WritePacket(single byte) failed, %v", err)
	}

	data, err := c.ReadPacketReuseMem(nil)
	if err != nil {
		return moerr.NewInternalErrorf(context.Background(), "ReadPacket failed, %v", err)
	}

	block, _ := pem.Decode(data[1:])
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return moerr.NewInternalErrorf(context.Background(), "x509.ParsePKIXPublicKey failed, %v", err)
	}

	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(cipher)
		plain[i] ^= cipher[j]
	}
	sha1v := sha1.New()
	enc, _ := rsa.EncryptOAEP(sha1v, rand.Reader, pub.(*rsa.PublicKey), plain, nil)
	data = make([]byte, 4+len(enc))
	copy(data[4:], enc)
	return moerr.NewInternalErrorf(context.Background(), "WritePacket failed, %v", c.WritePacket(data))
}

func EncryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1v := sha1.New()
	return rsa.EncryptOAEP(sha1v, rand.Reader, pub, plain, nil)
}

func (c *RConn) WriteEncryptedPassword(password string, seed []byte, pub *rsa.PublicKey) error {
	enc, err := EncryptPassword(password, seed, pub)
	if err != nil {
		return moerr.NewInternalErrorf(context.Background(), "EncryptPassword failed, %v", err)
	}
	return moerr.NewInternalErrorf(context.Background(), "WriteAuthSwitchPacket faile, %v", c.WriteAuthSwitchPacket(enc, false))
}

func (c *RConn) handleAuthResult() error {
	data, switchToPlugin, err := c.readAuthResult()
	if err != nil {
		return moerr.NewInternalErrorf(context.Background(), "readAuthResult: %w", err)
	}
	// handle auth switch, only support 'sha256_password', and 'caching_sha2_password'
	if switchToPlugin != "" {
		// fmt.Printf("now switching auth plugin to '%s'\n", switchToPlugin)
		if data == nil {
			data = c.salt
		} else {
			copy(c.salt, data)
		}
		c.authPluginName = switchToPlugin
		auth, addNull, err := c.genAuthResponse(data)
		if err != nil {
			return err
		}

		if err = c.WriteAuthSwitchPacket(auth, addNull); err != nil {
			return err
		}

		// Read Result Packet
		data, switchToPlugin, err = c.readAuthResult()
		if err != nil {
			return err
		}

		// Do not allow to change the auth plugin more than once
		if switchToPlugin != "" {
			return moerr.NewInternalError(context.Background(), "can not switch auth plugin more than once")
		}
	}

	// handle caching_sha2_password
	if c.authPluginName == AUTH_CACHING_SHA2_PASSWORD {
		if data == nil {
			return nil // auth already succeeded
		}
		if data[0] == CACHE_SHA2_FAST_AUTH {
			_, err = c.readOK()
			return err
		} else if data[0] == CACHE_SHA2_FULL_AUTH {
			// need full authentication
			if err = c.WritePublicKeyAuthPacket(c.password, c.salt); err != nil {
				return err
			}
			_, err = c.readOK()
			return err
		} else {
			return moerr.NewInternalErrorf(context.Background(), "invalid packet %x", data[0])
		}
	} else if c.authPluginName == AUTH_SHA256_PASSWORD {
		if len(data) == 0 {
			return nil // auth already succeeded
		}
		block, _ := pem.Decode(data)
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return err
		}
		// send encrypted password
		err = c.WriteEncryptedPassword(c.password, c.salt, pub.(*rsa.PublicKey))
		if err != nil {
			return err
		}
		_, err = c.readOK()
		return err
	}
	return nil
}

func (c *RConn) handshake() error {
	var err error
	if err = c.readInitialHandshake(); err != nil {
		c.Close()
		return moerr.NewInternalErrorf(context.Background(), "readInitialHandshake: %w", err)
	}

	if err = c.writeAuthHandshake(); err != nil {
		c.Close()
		return moerr.NewInternalErrorf(context.Background(), "writeAuthHandshake: %w", err)
	}

	if err = c.handleAuthResult(); err != nil {
		c.Close()
		return moerr.NewInternalErrorf(context.Background(), "handleAuthResult: %w", err)
	}

	return nil
}

// ConnectWithDialer to a MySQL server using the given Dialer.
func ConnectWithDialer(addr, user, password, dbName string, options ...Option) (*RConn, error) {
	c := new(RConn)

	c.BufferSize = defaultBufferSize
	c.attributes = map[string]string{
		"_client_name":     "mo-client",
		"_os":              runtime.GOOS,
		"_platform":        runtime.GOARCH,
		"_runtime_version": runtime.Version(),
	}

	var err error
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c.user = user
	c.password = password
	c.db = dbName

	// use default charset here, utf-8
	c.charset = "utf-8"

	// Apply configuration functions.
	for _, option := range options {
		if err := option(c); err != nil {
			// must close the connection in the event the provided configuration is not valid
			_ = conn.Close()
			return nil, err
		}
	}

	c.Conn = conn
	c.br = bufio.NewReaderSize(c, c.BufferSize)
	c.reader = c.br
	c.copyNBuf = make([]byte, 16*1024)

	if err = c.handshake(); err != nil {
		// in the event of an error c.handshake() will close the connection
		return nil, err
	}

	return c, nil
}

func (b *BinlogSyncer) newConnection() (*RConn, error) {
	var addr string
	addr = net.JoinHostPort(b.cfg.Host, strconv.Itoa(int(b.cfg.Port)))

	return ConnectWithDialer(addr, b.cfg.User, b.cfg.Password, "test", func(c *RConn) error {
		c.SetAttributes(map[string]string{"_client_role": "binary_log_listener"})
		return nil
	})
}

func (c *RConn) Close() error {
	c.Sequence = 0
	if c.Conn != nil {
		c.Conn.Close()
		return moerr.NewInternalError(context.Background(), "Conn.Close failed")
	}
	return nil
}

func (c *RConn) GetConnectionID() uint32 {
	return c.connectionID
}

func ByteSliceGet(length int) *ByteSlice {
	data := byteSlicePool.Get().(*ByteSlice)
	if cap(*data) < length {
		*data = make([]byte, length)
	} else {
		*data = (*data)[:length]
	}
	return data
}

func ByteSlicePut(data *ByteSlice) {
	*data = (*data)[:0]
	byteSlicePool.Put(data)
}

func (c *RConn) writeCommandBuf(command byte, arg []byte) error {
	c.Sequence = 0

	length := len(arg) + 1
	data := ByteSliceGet(length + 4)
	(*data)[4] = command

	copy((*data)[5:], arg)

	err := c.WritePacket(*data)

	ByteSlicePut(data)

	return err
}

func StringToByteSlice(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func (c *RConn) writeCommandStr(command byte, arg string) error {
	return c.writeCommandBuf(command, StringToByteSlice(arg))
}

func (r *Resultset) Reset(fieldsCount int) {
	r.RawPkg = r.RawPkg[:0]

	r.Fields = r.Fields[:0]
	r.Values = r.Values[:0]
	r.RowDatas = r.RowDatas[:0]

	if r.FieldNames != nil {
		for k := range r.FieldNames {
			delete(r.FieldNames, k)
		}
	} else {
		r.FieldNames = make(map[string]int)
	}

	if fieldsCount == 0 {
		return
	}

	if cap(r.Fields) < fieldsCount {
		r.Fields = make([]*Field, fieldsCount)
	} else {
		r.Fields = r.Fields[:fieldsCount]
	}
}

func NewResultset(fieldsCount int) *Resultset {
	r := resultsetPool.Get().(*Resultset)
	r.Reset(fieldsCount)
	return r
}

func (c *RConn) isEOFPacket(data []byte) bool {
	return data[0] == EOF_HEADER && len(data) <= 5
}

func SkipLengthEncodedString(b []byte) (int, error) {
	// Get length
	num, _, n := LengthEncodedInt(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

// LengthEncodedString returns the string read as a bytes slice, whether the value is NULL,
// the number of bytes read and an error, in case the string is longer than
// the input slice
func LengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := LengthEncodedInt(b)
	if num < 1 {
		return b[n:n], isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n : n], false, n, nil
	}
	return nil, false, n, io.EOF
}

func (f *Field) Parse(p []byte) (err error) {
	f.Data = p

	var n int
	pos := 0
	//skip catelog, always def
	n, err = SkipLengthEncodedString(p)
	if err != nil {
		return err
	}
	pos += n

	//schema
	f.Schema, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return err
	}
	pos += n

	//table
	f.Table, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return err
	}
	pos += n

	//org_table
	f.OrgTable, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return err
	}
	pos += n

	//name
	f.Name, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return err
	}
	pos += n

	//org_name
	f.OrgName, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return err
	}
	pos += n

	//skip oc
	pos += 1

	//charset
	f.Charset = binary.LittleEndian.Uint16(p[pos:])
	pos += 2

	//column length
	f.ColumnLength = binary.LittleEndian.Uint32(p[pos:])
	pos += 4

	//type
	f.Type = p[pos]
	pos++

	//flag
	f.Flag = binary.LittleEndian.Uint16(p[pos:])
	pos += 2

	//decimals 1
	f.Decimal = p[pos]
	pos++

	//filter [0x00][0x00]
	pos += 2

	f.DefaultValue = nil
	//if more data, command was field list
	if len(p) > pos {
		//length of default value lenenc-int
		f.DefaultValueLength, _, n = LengthEncodedInt(p[pos:])
		pos += n

		if pos+int(f.DefaultValueLength) > len(p) {
			err = moerr.NewInternalError(context.Background(), "Malform packet error")
			return err
		}

		//default value string[$len]
		f.DefaultValue = p[pos:(pos + int(f.DefaultValueLength))]
	}

	return nil
}

func (c *RConn) readResultColumns(result *Result) (err error) {
	var i = 0
	var data []byte

	for {
		rawPkgLen := len(result.RawPkg)
		result.RawPkg, err = c.ReadPacketReuseMem(result.RawPkg)
		if err != nil {
			return err
		}
		data = result.RawPkg[rawPkgLen:]

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {
				result.Warnings = binary.LittleEndian.Uint16(data[1:])
				// todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			if i != len(result.Fields) {
				err = moerr.NewInternalError(context.Background(), "Malform packet error")
			}

			return err
		}

		if result.Fields[i] == nil {
			result.Fields[i] = &Field{}
		}
		err = result.Fields[i].Parse(data)
		if err != nil {
			return err
		}

		result.FieldNames[hack.String(result.Fields[i].Name)] = i

		i++
	}
}

func ParseBinaryUint8(data []byte) uint8 {
	return data[0]
}

func ParseBinaryInt8(data []byte) int8 {
	return int8(data[0])
}

func ParseBinaryInt16(data []byte) int16 {
	return int16(binary.LittleEndian.Uint16(data))
}

func ParseBinaryUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func ParseBinaryInt32(data []byte) int32 {
	return int32(binary.LittleEndian.Uint32(data))
}

func ParseBinaryUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func ParseBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}

func ParseBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func ParseBinaryFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func ParseBinaryFloat64(data []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data))
}

func FormatBinaryDate(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	default:
		return nil, moerr.NewInternalErrorf(context.Background(), "invalid date packet length %d", n)
	}
}

func FormatBinaryDateTime(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00 00:00:00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d 00:00:00",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	case 7:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6])), nil
	case 11:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d.%06d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6],
			binary.LittleEndian.Uint32(data[7:11]))), nil
	default:
		return nil, moerr.NewInternalErrorf(context.Background(), "invalid datetime packet length %d", n)
	}
}

func FormatBinaryTime(n int, data []byte) ([]byte, error) {
	if n == 0 {
		return []byte("00:00:00"), nil
	}

	var sign byte
	if data[0] == 1 {
		sign = byte('-')
	}

	var bytes []byte
	switch n {
	case 8:
		bytes = []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
		))
	case 12:
		bytes = []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d.%06d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
			binary.LittleEndian.Uint32(data[8:12]),
		))
	default:
		return nil, moerr.NewInternalErrorf(context.Background(), "invalid time packet length %d", n)
	}
	if bytes[0] == 0 {
		return bytes[1:], nil
	}
	return bytes, nil
}

func Uint64ToInt64(val uint64) int64 {
	return *(*int64)(unsafe.Pointer(&val))
}

func Uint64ToFloat64(val uint64) float64 {
	return *(*float64)(unsafe.Pointer(&val))
}

func Int64ToUint64(val int64) uint64 {
	return *(*uint64)(unsafe.Pointer(&val))
}

func Float64ToUint64(val float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&val))
}

// ParseBinary parses the binary format of data
// see https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
func (p RowData) ParseBinary(f []*Field, dst []FieldValue) ([]FieldValue, error) {
	for len(dst) < len(f) {
		dst = append(dst, FieldValue{})
	}
	data := dst[:len(f)]

	if p[0] != OK_HEADER {
		return nil, moerr.NewInternalError(context.Background(), "Malform packet error")
	}

	pos := 1 + ((len(f) + 7 + 2) >> 3)

	nullBitmap := p[1:pos]

	var isNull bool
	var n int
	var err error
	var v []byte
	for i := range data {
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			data[i].Type = FieldValueTypeNull
			continue
		}

		isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0

		switch f[i].Type {
		case MYSQL_TYPE_NULL:
			data[i].Type = FieldValueTypeNull
			continue

		case MYSQL_TYPE_TINY:
			if isUnsigned {
				v := ParseBinaryUint8(p[pos : pos+1])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = uint64(v)
			} else {
				v := ParseBinaryInt8(p[pos : pos+1])
				data[i].Type = FieldValueTypeSigned
				data[i].value = Int64ToUint64(int64(v))
			}
			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if isUnsigned {
				v := ParseBinaryUint16(p[pos : pos+2])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = uint64(v)
			} else {
				v := ParseBinaryInt16(p[pos : pos+2])
				data[i].Type = FieldValueTypeSigned
				data[i].value = Int64ToUint64(int64(v))
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
			if isUnsigned {
				v := ParseBinaryUint32(p[pos : pos+4])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = uint64(v)
			} else {
				v := ParseBinaryInt32(p[pos : pos+4])
				data[i].Type = FieldValueTypeSigned
				data[i].value = Int64ToUint64(int64(v))
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
			if isUnsigned {
				v := ParseBinaryUint64(p[pos : pos+8])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = v
			} else {
				v := ParseBinaryInt64(p[pos : pos+8])
				data[i].Type = FieldValueTypeSigned
				data[i].value = Int64ToUint64(v)
			}
			pos += 8
			continue

		case MYSQL_TYPE_FLOAT:
			v := ParseBinaryFloat32(p[pos : pos+4])
			data[i].Type = FieldValueTypeFloat
			data[i].value = Float64ToUint64(float64(v))
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			v := ParseBinaryFloat64(p[pos : pos+8])
			data[i].Type = FieldValueTypeFloat
			data[i].value = Float64ToUint64(v)
			pos += 8
			continue

		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_JSON:
			v, isNull, n, err = LengthEncodedString(p[pos:])
			pos += n
			if err != nil {
				return nil, err
			}

			if !isNull {
				data[i].Type = FieldValueTypeString
				data[i].str = append(data[i].str[:0], v...)
				continue
			} else {
				data[i].Type = FieldValueTypeNull
				continue
			}

		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].str, err = FormatBinaryDate(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].str, err = FormatBinaryDateTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].str, err = FormatBinaryTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		default:
			return nil, moerr.NewInternalErrorf(context.Background(), "Stmt Unknown FieldType %d %s", f[i].Type, f[i].Name)
		}
	}

	return data, nil
}

func ByteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (p RowData) ParseText(f []*Field, dst []FieldValue) ([]FieldValue, error) {
	for len(dst) < len(f) {
		dst = append(dst, FieldValue{})
	}
	data := dst[:len(f)]

	var err error
	var v []byte
	var isNull bool
	var pos, n int

	for i := range f {
		v, isNull, n, err = LengthEncodedString(p[pos:])
		if err != nil {
			return nil, err
		}

		pos += n

		if isNull {
			data[i].Type = FieldValueTypeNull
		} else {
			isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0

			switch f[i].Type {
			case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG, MYSQL_TYPE_LONG, MYSQL_TYPE_YEAR:
				if isUnsigned {
					var val uint64
					data[i].Type = FieldValueTypeUnsigned
					val, err = strconv.ParseUint(ByteSliceToString(v), 10, 64)
					data[i].value = val
				} else {
					var val int64
					data[i].Type = FieldValueTypeSigned
					val, err = strconv.ParseInt(ByteSliceToString(v), 10, 64)
					data[i].value = Int64ToUint64(val)
				}
			case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
				var val float64
				data[i].Type = FieldValueTypeFloat
				val, err = strconv.ParseFloat(ByteSliceToString(v), 64)
				data[i].value = Float64ToUint64(val)
			default:
				data[i].Type = FieldValueTypeString
				data[i].str = append(data[i].str[:0], v...)
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return data, nil
}

func (p RowData) Parse(f []*Field, binary bool, dst []FieldValue) ([]FieldValue, error) {
	if binary {
		return p.ParseBinary(f, dst)
	} else {
		return p.ParseText(f, dst)
	}
}

func (c *RConn) readResultRows(result *Result, isBinary bool) (err error) {
	var data []byte

	for {
		rawPkgLen := len(result.RawPkg)
		result.RawPkg, err = c.ReadPacketReuseMem(result.RawPkg)
		if err != nil {
			return err
		}
		data = result.RawPkg[rawPkgLen:]

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {
				result.Warnings = binary.LittleEndian.Uint16(data[1:])
				// todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			break
		}

		if data[0] == ERR_HEADER {
			return moerr.NewInternalError(context.Background(), "invalid packet")
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	if cap(result.Values) < len(result.RowDatas) {
		result.Values = make([][]FieldValue, len(result.RowDatas))
	} else {
		result.Values = result.Values[:len(result.RowDatas)]
	}

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, isBinary, result.Values[i])

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *RConn) readResultset(data []byte, binary bool) (*Result, error) {
	// column count
	count, _, n := LengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, moerr.NewInternalError(context.Background(), "Malform packet error")
	}

	result := &Result{
		Resultset: NewResultset(int(count)),
	}

	if err := c.readResultColumns(result); err != nil {
		return nil, err
	}

	if err := c.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *RConn) readResult(binary bool) (*Result, error) {
	bs := ByteSliceGet(16)
	defer ByteSlicePut(bs)
	var err error
	*bs, err = c.ReadPacketReuseMem((*bs)[:0])
	if err != nil {
		return nil, err
	}

	switch (*bs)[0] {
	case OK_HEADER:
		return c.handleOKPacket(*bs)
	case ERR_HEADER:
		return nil, moerr.NewInternalError(context.Background(), "invalid packet")
	case LocalInFile_HEADER:
		return nil, moerr.NewInternalError(context.Background(), "Malform packet error")
	default:
		return c.readResultset(*bs, binary)
	}
}

func (c *RConn) exec(query string) (*Result, error) {
	if err := c.writeCommandStr(byte(COM_QUERY), query); err != nil {
		return nil, err
	}

	return c.readResult(false)
}

func (c *RConn) readUntilEOF() (err error) {
	var data []byte

	for {
		data, err = c.ReadPacketReuseMem(nil)

		if err != nil {
			return
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			return
		}
	}
}

func (c *RConn) Prepare(query string) (*Stmt, error) {
	if err := c.writeCommandStr(byte(COM_STMT_PREPARE), query); err != nil {
		return nil, err
	}

	data, err := c.ReadPacketReuseMem(nil)
	if err != nil {
		return nil, err
	}

	if data[0] == ERR_HEADER {
		return nil, moerr.NewInternalError(context.Background(), "invalid packet")
	} else if data[0] != OK_HEADER {
		return nil, moerr.NewInternalError(context.Background(), "Malform packet error")
	}

	s := new(Stmt)
	s.conn = c

	pos := 1

	//for statement id
	s.id = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	//number columns
	s.columns = int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	//number params
	s.params = int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	//warnings
	s.warnings = int(binary.LittleEndian.Uint16(data[pos:]))
	// pos += 2

	if s.params > 0 {
		if err := s.conn.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	if s.columns > 0 {
		if err := s.conn.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func Uint16ToBytes(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func Uint32ToBytes(n uint32) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func Uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func (s *Stmt) write(args ...interface{}) error {
	paramsNum := s.params

	if len(args) != paramsNum {
		return moerr.NewInternalErrorf(context.Background(), "argument mismatch, need %d but got %d", s.params, len(args))
	}

	paramTypes := make([]byte, paramsNum<<1)
	paramValues := make([][]byte, paramsNum)

	//NULL-bitmap, length: (num-params+7)
	nullBitmap := make([]byte, (paramsNum+7)>>3)

	length := 1 + 4 + 1 + 4 + ((paramsNum + 7) >> 3) + 1 + (paramsNum << 1)

	var newParamBoundFlag byte = 0

	for i := range args {
		if args[i] == nil {
			nullBitmap[i/8] |= 1 << (uint(i) % 8)
			paramTypes[i<<1] = MYSQL_TYPE_NULL
			continue
		}

		newParamBoundFlag = 1

		switch v := args[i].(type) {
		case int8:
			paramTypes[i<<1] = MYSQL_TYPE_TINY
			paramValues[i] = []byte{byte(v)}
		case int16:
			paramTypes[i<<1] = MYSQL_TYPE_SHORT
			paramValues[i] = Uint16ToBytes(uint16(v))
		case int32:
			paramTypes[i<<1] = MYSQL_TYPE_LONG
			paramValues[i] = Uint32ToBytes(uint32(v))
		case int:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramValues[i] = Uint64ToBytes(uint64(v))
		case int64:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramValues[i] = Uint64ToBytes(uint64(v))
		case uint8:
			paramTypes[i<<1] = MYSQL_TYPE_TINY
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = []byte{v}
		case uint16:
			paramTypes[i<<1] = MYSQL_TYPE_SHORT
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint16ToBytes(v)
		case uint32:
			paramTypes[i<<1] = MYSQL_TYPE_LONG
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint32ToBytes(v)
		case uint:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint64ToBytes(uint64(v))
		case uint64:
			paramTypes[i<<1] = MYSQL_TYPE_LONGLONG
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint64ToBytes(v)
		case bool:
			paramTypes[i<<1] = MYSQL_TYPE_TINY
			if v {
				paramValues[i] = []byte{1}
			} else {
				paramValues[i] = []byte{0}
			}
		case float32:
			paramTypes[i<<1] = MYSQL_TYPE_FLOAT
			paramValues[i] = Uint32ToBytes(math.Float32bits(v))
		case float64:
			paramTypes[i<<1] = MYSQL_TYPE_DOUBLE
			paramValues[i] = Uint64ToBytes(math.Float64bits(v))
		case string:
			paramTypes[i<<1] = MYSQL_TYPE_STRING
			paramValues[i] = append(PutLengthEncodedInt(uint64(len(v))), v...)
		case []byte:
			paramTypes[i<<1] = MYSQL_TYPE_STRING
			paramValues[i] = append(PutLengthEncodedInt(uint64(len(v))), v...)
		case json.RawMessage:
			paramTypes[i<<1] = MYSQL_TYPE_STRING
			paramValues[i] = append(PutLengthEncodedInt(uint64(len(v))), v...)
		default:
			return moerr.NewInternalErrorf(context.Background(), "invalid argument type %T", args[i])
		}

		length += len(paramValues[i])
	}

	data := BytesBufferGet()
	defer func() {
		BytesBufferPut(data)
	}()
	if data.Len() < length+4 {
		data.Grow(4 + length)
	}

	data.Write([]byte{0, 0, 0, 0})
	data.WriteByte(byte(COM_STMT_EXECUTE))
	data.Write([]byte{byte(s.id), byte(s.id >> 8), byte(s.id >> 16), byte(s.id >> 24)})

	//flag: CURSOR_TYPE_NO_CURSOR
	data.WriteByte(0x00)

	//iteration-count, always 1
	data.Write([]byte{1, 0, 0, 0})

	if s.params > 0 {
		data.Write(nullBitmap)

		//new-params-bound-flag
		data.WriteByte(newParamBoundFlag)

		if newParamBoundFlag == 1 {
			//type of each parameter, length: num-params * 2
			data.Write(paramTypes)

			//value of each parameter
			for _, v := range paramValues {
				data.Write(v)
			}
		}
	}

	s.conn.Sequence = 0

	return s.conn.WritePacket(data.Bytes())
}

func (s *Stmt) Execute(args ...interface{}) (*Result, error) {
	if err := s.write(args...); err != nil {
		return nil, err
	}

	return s.conn.readResult(true)
}

func (c *RConn) writeCommandUint32(command byte, arg uint32) error {
	c.Sequence = 0

	buf := ByteSliceGet(9)

	(*buf)[0] = 0x05 //5 bytes long
	(*buf)[1] = 0x00
	(*buf)[2] = 0x00
	(*buf)[3] = 0x00 //sequence

	(*buf)[4] = command

	(*buf)[5] = byte(arg)
	(*buf)[6] = byte(arg >> 8)
	(*buf)[7] = byte(arg >> 16)
	(*buf)[8] = byte(arg >> 24)

	err := c.WritePacket((*buf))
	ByteSlicePut(buf)
	return err
}

func (s *Stmt) Close() error {
	if err := s.conn.writeCommandUint32(byte(COM_STMT_CLOSE), s.id); err != nil {
		return err
	}

	return nil
}

func (c *RConn) Execute(command string, args ...interface{}) (*Result, error) {
	if len(args) == 0 {
		return c.exec(command)
	} else {
		if s, err := c.Prepare(command); err != nil {
			return nil, err
		} else {
			var r *Result
			r, err = s.Execute(args...)
			s.Close()
			return r, err
		}
	}
}

func (fv *FieldValue) AsUint64() uint64 {
	return fv.value
}

func (fv *FieldValue) AsInt64() int64 {
	return Uint64ToInt64(fv.value)
}

func (fv *FieldValue) AsFloat64() float64 {
	return Uint64ToFloat64(fv.value)
}

func (fv *FieldValue) AsString() []byte {
	return fv.str
}

func (fv *FieldValue) Value() interface{} {
	switch fv.Type {
	case FieldValueTypeUnsigned:
		return fv.AsUint64()
	case FieldValueTypeSigned:
		return fv.AsInt64()
	case FieldValueTypeFloat:
		return fv.AsFloat64()
	case FieldValueTypeString:
		return fv.AsString()
	default: // FieldValueTypeNull
		return nil
	}
}

func (r *Resultset) GetValue(row, column int) (interface{}, error) {
	if row >= len(r.Values) || row < 0 {
		return nil, moerr.NewInternalErrorf(context.Background(), "invalid row index %d", row)
	}

	if column >= len(r.Fields) || column < 0 {
		return nil, moerr.NewInternalErrorf(context.Background(), "invalid column index %d", column)
	}

	return r.Values[row][column].Value(), nil
}

func (r *Resultset) GetString(row, column int) (string, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return "", err
	}

	switch v := d.(type) {
	case string:
		return v, nil
	case []byte:
		return hack.String(v), nil
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", nil
	default:
		return "", moerr.NewInternalErrorf(context.Background(), "data type is %T", v)
	}
}

func (b *BinlogSyncer) writeRegisterSlaveCommand() error {
	b.c.Sequence = 0

	hostname, _ := os.Hostname()

	// This should be the name of slave host not the host we are connecting to.
	data := make([]byte, 4+1+4+1+len(hostname)+1+len(b.cfg.User)+1+len(b.cfg.Password)+2+4+4)
	pos := 4

	data[pos] = byte(COM_REGISTER_SLAVE)
	pos++

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
	pos += 4

	// This should be the name of slave hostname not the host we are connecting to.
	data[pos] = uint8(len(hostname))
	pos++
	n := copy(data[pos:], hostname)
	pos += n

	data[pos] = uint8(len(b.cfg.User))
	pos++
	n = copy(data[pos:], b.cfg.User)
	pos += n

	data[pos] = uint8(len(b.cfg.Password))
	pos++
	n = copy(data[pos:], b.cfg.Password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], b.cfg.Port)
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) registerSlave() error {
	if b.c != nil {
		b.c.Close()
	}
	var err error
	b.c, err = b.newConnection()
	if err != nil {
		return err
	}

	// save last last connection id for kill
	b.lastConnectionID = b.c.GetConnectionID()

	//for mysql 5.6+, binlog has a crc32 checksum
	//before mysql 5.6, this will not work, don't matter.:-)
	if r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"); err != nil {
		return err
	} else {
		s, _ := r.GetString(0, 1)
		if s != "" {
			// maybe CRC32 or NONE

			// mysqlbinlog.cc use NONE, see its below comments:
			// Make a notice to the server that this client
			// is checksum-aware. It does not need the first fake Rotate
			// necessary checksummed.
			// That preference is specified below.

			if _, err = b.c.Execute(`SET @master_binlog_checksum='NONE'`); err != nil {
				return err
			}

			// if _, err = b.c.Execute(`SET @master_binlog_checksum=@@global.binlog_checksum`); err != nil {
			// 	return errors.Trace(err)
			// }
		}
	}

	if err = b.writeRegisterSlaveCommand(); err != nil {
		return err
	}

	if _, err = b.c.readOK(); err != nil {
		return err
	}

	serverUUID, err := uuid.NewUUID()
	if err != nil {
		logutil.Errorf("failed to get new uuid %v", err)
		return err
	}
	if _, err = b.c.Execute(fmt.Sprintf("SET @slave_uuid = '%s', @replica_uuid = '%s'", serverUUID, serverUUID)); err != nil {
		logutil.Errorf("failed to set @slave_uuid = '%s', err: %v", serverUUID, err)
		return err
	}

	return nil
}

func (c *RConn) GetServerVersion() string {
	return c.serverVersion
}

func (b *BinlogSyncer) prepare() error {
	if b.isClosed() {
		return moerr.NewInternalError(context.Background(), "sync was closed")
	}

	if err := b.registerSlave(); err != nil {
		return err
	}

	logutil.Infof("Connected to mysql %s server", b.c.GetServerVersion())

	return nil
}

func (s *UUIDSet) encode(w io.Writer) {
	b, _ := s.SID.MarshalBinary()

	_, _ = w.Write(b)
	n := int64(len(s.Intervals))

	_ = binary.Write(w, binary.LittleEndian, n)

	for _, i := range s.Intervals {
		_ = binary.Write(w, binary.LittleEndian, i.Start)
		_ = binary.Write(w, binary.LittleEndian, i.Stop)
	}
}

func (s GTIDSet) Encode() []byte {
	var buf bytes.Buffer

	_ = binary.Write(&buf, binary.LittleEndian, uint64(len(s)))

	for i := range s {
		s[i].encode(&buf)
	}

	return buf.Bytes()
}

func (b *BinlogSyncer) writeBinlogDumpMysqlGTIDCommand(gset GTIDSet) error {
	p := Position{Name: "", Pos: 4}
	gtidData := gset.Encode()

	b.c.Sequence = 0

	data := make([]byte, 4+1+2+4+4+len(p.Name)+8+4+len(gtidData))
	pos := 4
	data[pos] = byte(COM_BINLOG_DUMP_GTID)
	pos++

	binary.LittleEndian.PutUint16(data[pos:], 0)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(p.Name)))
	pos += 4

	n := copy(data[pos:], p.Name)
	pos += n

	binary.LittleEndian.PutUint64(data[pos:], uint64(p.Pos))
	pos += 8

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(gtidData)))
	pos += 4
	n = copy(data[pos:], gtidData)
	pos += n

	data = data[0:pos]

	return b.c.WritePacket(data)
}

func NewBinlogStreamerWithChanSize(chanSize int) *BinlogStreamer {
	s := new(BinlogStreamer)

	if chanSize <= 0 {
		chanSize = 10240
	}

	s.ch = make(chan *BinlogEvent, chanSize)
	s.ech = make(chan error, 4)

	return s
}

func (s *BinlogStreamer) closeWithError(err error) {
	if err == nil {
		err = moerr.NewInternalError(context.Background(), "Sync was closed")
	} else {
		logutil.Errorf("close sync with err: %v", err)
	}

	select {
	case s.ech <- err:
	default:
	}
}

func Pstack() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return string(buf[0:n])
}

func (h *EventHeader) Decode(data []byte) error {
	if len(data) < EventHeaderSize {
		return moerr.NewInternalErrorf(context.Background(), "header size too short %d, must 19", len(data))
	}

	pos := 0

	h.Timestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventType = EventType(data[pos])
	pos++

	h.ServerID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.LogPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.Flags = binary.LittleEndian.Uint16(data[pos:])
	// pos += 2

	if h.EventSize < uint32(EventHeaderSize) {
		return moerr.NewInternalErrorf(context.Background(), "invalid event size %d, must >= 19", h.EventSize)
	}

	return nil
}

func (p *BinlogParser) parseHeader(data []byte) (*EventHeader, error) {
	h := new(EventHeader)
	err := h.Decode(data)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (p *BinlogParser) verifyCrc32Checksum(rawData []byte) error {
	if !p.verifyChecksum {
		return nil
	}

	calculatedPart := rawData[0 : len(rawData)-BinlogChecksumLength]
	expectedChecksum := rawData[len(rawData)-BinlogChecksumLength:]

	// mysql use zlib's CRC32 implementation, which uses polynomial 0xedb88320UL.
	// reference: https://github.com/madler/zlib/blob/master/crc32.c
	// https://github.com/madler/zlib/blob/master/doc/rfc1952.txt#L419
	checksum := crc32.ChecksumIEEE(calculatedPart)
	computed := make([]byte, BinlogChecksumLength)
	binary.LittleEndian.PutUint32(computed, checksum)
	if !bytes.Equal(expectedChecksum, computed) {
		return moerr.NewInternalError(context.Background(), "binlog checksum mismatch, data may be corrupted")
	}
	return nil
}

func (p *BinlogParser) newRowsEvent(h *EventHeader) *RowsEvent {
	e := &RowsEvent{}

	postHeaderLen := p.format.EventTypeHeaderLengths[h.EventType-1]
	if postHeaderLen == 6 {
		e.tableIDSize = 4
	} else {
		e.tableIDSize = 6
	}

	e.needBitmap2 = false
	e.tables = p.tables
	e.eventType = h.EventType
	e.parseTime = p.parseTime
	e.timestampStringLocation = p.timestampStringLocation
	e.useDecimal = p.useDecimal
	e.ignoreJSONDecodeErr = p.ignoreJSONDecodeErr

	switch h.EventType {
	case WRITE_ROWS_EVENTv0:
		e.Version = 0
	case UPDATE_ROWS_EVENTv0:
		e.Version = 0
	case DELETE_ROWS_EVENTv0:
		e.Version = 0
	case WRITE_ROWS_EVENTv1:
		e.Version = 1
	case DELETE_ROWS_EVENTv1:
		e.Version = 1
	case UPDATE_ROWS_EVENTv1:
		e.Version = 1
		e.needBitmap2 = true
	case WRITE_ROWS_EVENTv2:
		e.Version = 2
	case UPDATE_ROWS_EVENTv2:
		e.Version = 2
		e.needBitmap2 = true
	case DELETE_ROWS_EVENTv2:
		e.Version = 2
	case PARTIAL_UPDATE_ROWS_EVENT:
		e.Version = 2
		e.needBitmap2 = true
	}

	return e
}

func (p *BinlogParser) newTransactionPayloadEvent() *TransactionPayloadEvent {
	e := &TransactionPayloadEvent{}
	e.format = *p.format

	return e
}

func (p *BinlogParser) parseEvent(h *EventHeader, data []byte, rawData []byte) (Event, error) {
	var e Event

	if h.EventType == FORMAT_DESCRIPTION_EVENT {
		p.format = &FormatDescriptionEvent{}
		e = p.format
	} else {
		if p.format != nil && p.format.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
			err := p.verifyCrc32Checksum(rawData)
			if err != nil {
				return nil, err
			}
			data = data[0 : len(data)-BinlogChecksumLength]
		}

		if h.EventType == ROTATE_EVENT {
			e = &RotateEvent{}
		} else if !p.rawMode {
			switch h.EventType {
			case QUERY_EVENT:
				e = &QueryEvent{}
			case XID_EVENT:
				e = &XIDEvent{}
			case TABLE_MAP_EVENT:
				te := &TableMapEvent{
					flavor:                 "mysql",
					optionalMetaDecodeFunc: p.tableMapOptionalMetaDecodeFunc,
				}
				if p.format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
					te.tableIDSize = 4
				} else {
					te.tableIDSize = 6
				}
				e = te
			case WRITE_ROWS_EVENTv0,
				UPDATE_ROWS_EVENTv0,
				DELETE_ROWS_EVENTv0,
				WRITE_ROWS_EVENTv1,
				DELETE_ROWS_EVENTv1,
				UPDATE_ROWS_EVENTv1,
				WRITE_ROWS_EVENTv2,
				UPDATE_ROWS_EVENTv2,
				DELETE_ROWS_EVENTv2,
				PARTIAL_UPDATE_ROWS_EVENT: // Extension of UPDATE_ROWS_EVENT, allowing partial values according to binlog_row_value_options
				e = p.newRowsEvent(h)
			case ROWS_QUERY_EVENT:
				e = &RowsQueryEvent{}
			case GTID_EVENT:
				e = &GTIDEvent{}
			case ANONYMOUS_GTID_EVENT:
				e = &GTIDEvent{}
			case BEGIN_LOAD_QUERY_EVENT:
				e = &BeginLoadQueryEvent{}
			case EXECUTE_LOAD_QUERY_EVENT:
				e = &ExecuteLoadQueryEvent{}
			case PREVIOUS_GTIDS_EVENT:
				e = &PreviousGTIDsEvent{}
			case INTVAR_EVENT:
				e = &IntVarEvent{}
			case TRANSACTION_PAYLOAD_EVENT:
				e = p.newTransactionPayloadEvent()
			default:
				e = &GenericEvent{}
			}
		} else {
			e = &GenericEvent{}
		}
	}

	var err error
	if re, ok := e.(*RowsEvent); ok && p.rowsEventDecodeFunc != nil {
		err = p.rowsEventDecodeFunc(re, data)
	} else {
		err = e.Decode(data)
	}
	if err != nil {
		return nil, &EventError{h, err.Error(), data}
	}

	if te, ok := e.(*TableMapEvent); ok {
		p.tables[te.TableID] = te
	}

	if re, ok := e.(*RowsEvent); ok {
		if (re.Flags & RowsEventStmtEndFlag) > 0 {
			// Refer https://github.com/alibaba/canal/blob/38cc81b7dab29b51371096fb6763ca3a8432ffee/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogEvent.java#L176
			p.tables = make(map[uint64]*TableMapEvent)
		}
	}

	return e, nil
}

// Parse: Given the bytes for a a binary log event: return the decoded event.
// With the exception of the FORMAT_DESCRIPTION_EVENT event type
// there must have previously been passed a FORMAT_DESCRIPTION_EVENT
// into the parser for this to work properly on any given event.
// Passing a new FORMAT_DESCRIPTION_EVENT into the parser will replace
// an existing one.
func (p *BinlogParser) Parse(data []byte) (*BinlogEvent, error) {
	rawData := data

	h, err := p.parseHeader(data)

	if err != nil {
		return nil, err
	}

	data = data[EventHeaderSize:]
	eventLen := int(h.EventSize) - EventHeaderSize

	if len(data) != eventLen {
		return nil, moerr.NewInternalErrorf(context.Background(), "invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
	}

	e, err := p.parseEvent(h, data, rawData)
	if err != nil {
		return nil, err
	}

	return &BinlogEvent{RawData: rawData, Header: h, Event: e}, nil
}

// parseEvent parses the raw data into a BinlogEvent.
// It only handles parsing and does not perform any side effects.
// Returns the parsed BinlogEvent, a boolean indicating if an ACK is needed, and an error if the
// parsing fails
func (b *BinlogSyncer) parseEvent(data []byte) (event *BinlogEvent, needACK bool, err error) {
	// Skip OK byte (0x00)
	data = data[1:]

	needACK = false

	// Parse the event using the BinlogParser
	event, err = b.parser.Parse(data)
	if err != nil {
		return nil, false, err
	}

	return event, needACK, nil
}

func (s *IntervalSlice) InsertInterval(interval Interval) {
	var (
		count int
		i     int
	)

	*s = append(*s, interval)
	total := len(*s)
	for i = total - 1; i > 0; i-- {
		if (*s)[i].Stop < (*s)[i-1].Start {
			(*s)[i], (*s)[i-1] = (*s)[i-1], (*s)[i]
		} else if (*s)[i].Start > (*s)[i-1].Stop {
			break
		} else {
			(*s)[i-1].Start = min((*s)[i-1].Start, (*s)[i].Start)
			(*s)[i-1].Stop = max((*s)[i-1].Stop, (*s)[i].Stop)
			count++
		}
	}
	if count > 0 {
		i++
		if i+count < total {
			copy((*s)[i:], (*s)[i+count:])
		}
		*s = (*s)[:total-count]
	}
}

func (s *GTIDSet) AddGTID(uuid uuid.UUID, gno int64) {
	sid := uuid.String()
	o, ok := (*s)[sid]
	if ok {
		o.Intervals.InsertInterval(Interval{gno, gno + 1})
	} else {
		(*s)[sid] = &UUIDSet{uuid, IntervalSlice{Interval{gno, gno + 1}}}
	}
}

// getCurrentGtidSet returns a clone of the current GTID set.
func (b *BinlogSyncer) getCurrentGtidSet() GTIDSet {
	if b.currGset != nil {
		return b.currGset.Clone()
	}
	return nil
}

func (s *UUIDSet) Clone() *UUIDSet {
	clone := new(UUIDSet)
	clone.SID = s.SID
	clone.Intervals = make([]Interval, len(s.Intervals))
	copy(clone.Intervals, s.Intervals)
	return clone
}

func (gtid GTIDSet) Clone() GTIDSet {
	clone := &GTIDSet{}
	for sid, uuidSet := range gtid {
		(*clone)[sid] = uuidSet.Clone()
	}

	return *clone
}

// handleEventAndACK processes an event and sends an ACK if necessary.
func (b *BinlogSyncer) handleEventAndACK(s *BinlogStreamer, e *BinlogEvent, needACK bool) error {
	// Update the next position based on the event's LogPos
	if e.Header.LogPos > 0 {
		// Some events like FormatDescriptionEvent return 0, ignore.
		b.nextPos.Pos = e.Header.LogPos
	}

	// Handle event types to update positions and GTID sets
	switch event := e.Event.(type) {
	case *RotateEvent:
		b.nextPos.Name = string(event.NextLogName)
		b.nextPos.Pos = uint32(event.Position)
		logutil.Infof("rotate to %s", b.nextPos)

	case *GTIDEvent:
		if b.prevGset == nil {
			break
		}
		if b.currGset == nil {
			b.currGset = b.prevGset.Clone()
		}
		u, err := uuid.FromBytes(event.SID)
		if err != nil {
			return err
		}
		(&b.currGset).AddGTID(u, event.GNO)
		if b.prevMySQLGTIDEvent != nil {
			u, err = uuid.FromBytes(b.prevMySQLGTIDEvent.SID)
			if err != nil {
				return err
			}
			(&b.prevGset).AddGTID(u, b.prevMySQLGTIDEvent.GNO)
		}
		b.prevMySQLGTIDEvent = event

	case *XIDEvent:
		if !b.cfg.DiscardGTIDSet {
			event.GSet = b.getCurrentGtidSet()
		}

	case *QueryEvent:
		if !b.cfg.DiscardGTIDSet {
			event.GSet = b.getCurrentGtidSet()
		}
	}

	// Asynchronous mode: send the event to the streamer channel
	select {
	case s.ch <- e:
	case <-b.ctx.Done():
		return moerr.NewInternalError(context.Background(), "sync is being closed...")
	}

	return nil
}

func (b *BinlogSyncer) onStream(s *BinlogStreamer) {
	defer func() {
		if e := recover(); e != nil {
			s.closeWithError(moerr.NewInternalErrorf(context.Background(), "Err: %v\n Stack: %s", e, Pstack()))
		}
		b.wg.Done()
	}()

	for {
		data, err := b.c.ReadPacketReuseMem(nil)
		select {
		case <-b.ctx.Done():
			s.closeWithError(nil)
			return
		default:
		}

		if err != nil {
			logutil.Errorf("%v", err)
			// we meet connection error, should re-connect again with
			// last nextPos or nextGTID we got.
			if len(b.nextPos.Name) == 0 && b.prevGset == nil {
				// we can't get the correct position, close.
				s.closeWithError(err)
				return
			}

			// we connect the server and begin to re-sync again.
			continue
		}

		// Reset retry count on successful packet receieve
		b.retryCount = 0

		switch data[0] {
		case OK_HEADER:
			// Parse the event
			e, needACK, err := b.parseEvent(data)
			if err != nil {
				s.closeWithError(err)
				return
			}

			// Handle the event and send ACK if necessary
			err = b.handleEventAndACK(s, e, needACK)
			if err != nil {
				s.closeWithError(err)
				return
			}
		case ERR_HEADER:
			err = moerr.NewInternalError(context.Background(), "invalid packet")
			s.closeWithError(err)
			return
		case EOF_HEADER:
			// refer to https://dev.mysql.com/doc/internals/en/com-binlog-dump.html#binlog-dump-non-block
			// when COM_BINLOG_DUMP command use BINLOG_DUMP_NON_BLOCK flag,
			// if there is no more event to send an EOF_Packet instead of blocking the connection
			logutil.Info("receive EOF packet, no more binlog event now.")
			continue
		default:
			logutil.Errorf("invalid stream header %c", data[0])
			continue
		}
	}
}

func (b *BinlogSyncer) startDumpStream() *BinlogStreamer {
	b.running = true

	s := NewBinlogStreamerWithChanSize(b.cfg.EventCacheCount)

	b.wg.Add(1)
	go b.onStream(s)
	return s
}

func (b *BinlogSyncer) StartSyncGTID(gset GTIDSet) (*BinlogStreamer, error) {
	logutil.Infof("begin to sync binlog from GTID set %s", gset)

	b.prevMySQLGTIDEvent = nil
	b.prevGset = gset

	b.m.Lock()
	defer b.m.Unlock()

	if b.running {
		return nil, moerr.NewInternalError(context.Background(), "last sync error or closed, try sync and get event again")
	}

	// establishing network connection here and will start getting binlog events from "gset + 1", thus until first
	// MariadbGTIDEvent/GTIDEvent event is received - we effectively do not have a "current GTID"
	b.currGset = nil

	if err := b.prepare(); err != nil {
		return nil, err
	}

	var err error
	err = b.writeBinlogDumpMysqlGTIDCommand(gset)

	if err != nil {
		return nil, err
	}

	return b.startDumpStream(), nil
}

// GetEvent gets the binlog event one by one, it will block until Syncer receives any events from MySQL
// or meets a sync error. You can pass a context (like Cancel or Timeout) to break the block.
func (s *BinlogStreamer) GetEvent(ctx context.Context) (*BinlogEvent, error) {
	if s.err != nil {
		return nil, moerr.NewInternalError(context.Background(), "Last sync error or closed, try sync and get event again")
	}

	select {
	case c := <-s.ch:
		return c, nil
	case s.err = <-s.ech:
		return nil, s.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (h *EventHeader) Dump(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", h.EventType)
	fmt.Fprintf(w, "Date: %s\n", time.Unix(int64(h.Timestamp), 0).Format(TimeFormat))
	fmt.Fprintf(w, "Log position: %d\n", h.LogPos)
	fmt.Fprintf(w, "Event size: %d\n", h.EventSize)
}

func (e *BinlogEvent) Dump(w io.Writer) {
	e.Header.Dump(w)
	e.Event.Dump(w)
}

func startReplication(ctx context.Context) {
	cfg := BinlogSyncerConfig{
		ServerID: 100,
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "snan",
		Password: "19990928",
	}
	syncer := NewBinlogSyncer(ctx, cfg)
	gtidSet, _ := GetGTIDSet()
	streamer, _ := syncer.StartSyncGTID(gtidSet)
	for {
		ev, _ := streamer.GetEvent(ctx)
		ev.Dump(os.Stdout)
	}
}
