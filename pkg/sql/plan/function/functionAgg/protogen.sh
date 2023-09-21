#!/bin/bash

WORKSPACE=$(dirname $(go env GOMOD))
VENDOR_DIR="$WORKSPACE/vendor"
PROTOC_DIR="$WORKSPACE/proto"

# https://github.com/gogo/protobuf.git@v1.3.2
${GOPATH}/bin/protoc -I=.:$PROTOC_DIR:$VENDOR_DIR --gogofast_out=paths=source_relative:. types.proto
goimports -w *.pb.go