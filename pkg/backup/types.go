package backup

import "github.com/matrixorigin/matrixone/pkg/fileservice"

const (
    Version       = "v1"
    BackupVersion = Version
)

type BackupConfig struct {
    // Backup Timestamp
    Timestamp string

    // output directory
    Directory string

    DirFS fileservice.FileService
}
