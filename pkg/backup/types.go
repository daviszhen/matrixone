package backup

import (
    "github.com/matrixorigin/matrixone/pkg/container/types"
    "github.com/matrixorigin/matrixone/pkg/fileservice"
    "github.com/matrixorigin/matrixone/pkg/logservice"
)

const (
    Version1 = "v1-20230714"
    Version  = Version1
)

type Config struct {
    // Timestamp
    Timestamp types.TS

    // For tae
    TaeDir fileservice.FileService

    // hakeeper client
    HAkeeper logservice.CNHAKeeperClient
}
