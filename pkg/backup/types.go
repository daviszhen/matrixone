package backup

import (
    "github.com/matrixorigin/matrixone/pkg/fileservice"
    "github.com/matrixorigin/matrixone/pkg/logservice"
)

const (
    Version1 = "v1-20230714"
    Version  = Version1
)

type Config struct {
    // Backup Timestamp
    Timestamp string

    // output directory
    Directory string

    DirFS fileservice.FileService

    // hakeeper client
    Hakeeper logservice.CNHAKeeperClient
}
