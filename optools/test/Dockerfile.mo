FROM golang:1.17.3 as builder

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone

WORKDIR /go/src/github.com/matrixorigin/matrixone

RUN go env -w GOPROXY=https://goproxy.cn,direct

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY . .

RUN make config && make build


FROM ubuntu

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-server /mo-server
COPY /optools/test/config.toml /system_vars_config.toml
COPY --chmod=755 /optools/test/entrypoint.sh /entrypoint.sh

WORKDIR /

EXPOSE 6001

ENTRYPOINT ["/entrypoint.sh"]
