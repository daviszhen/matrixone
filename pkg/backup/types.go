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

const (
    moMeta      = "mo_meta"
    buildDir    = "build"
    configDir   = "config"
    taeDir      = "tae"
    hakeeperDir = "hakeeper"
)

var (
    launchConfigPaths []string
)

type Config struct {
    // Timestamp
    Timestamp types.TS

    // For General usage
    GeneralDir fileservice.FileService

    // For tae
    TaeDir fileservice.FileService

    // hakeeper client
    HAkeeper logservice.CNHAKeeperClient
}

type s3Config struct {
    endpoint        string
    accessKeyId     string
    secretAccessKey string
    bucket          string
    filepath        string
    region          string
    compression     string
    roleArn         string
    provider        string
    externalId      string
    format          string
    jsonData        string
    isMinio         bool
}

type filesystemConfig struct {
    path string
}

type pathConfig struct {
    isS3   bool
    forETL bool
    s3Config
    filesystemConfig
}