package backup

import (
    "context"
    "github.com/google/uuid"
    "github.com/matrixorigin/matrixone/pkg/logutil"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
    "os"
    "path"
)

// Backup
// Note: ctx needs to support cancel. The user can cancel the backup task by canceling the ctx.
func Backup(ctx context.Context, bs *tree.BackupStart, cfg *Config) error {
    var err error
    var s3Conf *s3Config

    // step 1 : setup fileservice
    //1.1 setup ETL fileservice for general usage
    if !bs.IsS3 {
        cfg.GeneralDir, _, err = setupFilesystem(ctx, bs.Dir, true)
        if err != nil {
            return err
        }
        //TODO: for tae hakeeper

    } else {
        s3Conf, err = getS3Config(ctx, bs.Option)
        if err != nil {
            return err
        }
        cfg.GeneralDir, _, err = setupS3(ctx, s3Conf, true)
        if err != nil {
            return err
        }
        //TODO:
    }
    //TODO
    
    // step 2 : backup mo
    if err = backupBuildInfo(ctx, cfg); err != nil {
        return err
    }

    if err = backupConfigs(ctx, cfg); err != nil {
        return err
    }

    if err = backupTae(ctx, cfg); err != nil {
        return err
    }

    if err = backupHakeeper(ctx, cfg); err != nil {
        return err
    }

    return err
}

// saveBuildInfo saves backupVersion, build info.
func backupBuildInfo(ctx context.Context, cfg *Config) error {
    return writeFile(ctx, cfg.GeneralDir, moMeta, []byte(buildInfo()))
}

// saveConfigs saves cluster config or service config
func backupConfigs(ctx context.Context, cfg *Config) error {
    var err error
    // save cluster config files
    for _, f := range launchConfigPaths {
        err = backupFile(ctx, f, cfg)
        if err != nil {
            return err
        }
    }

    return err
}

func backupTae(ctx context.Context, config *Config) error {

    return nil
}

func backupHakeeper(ctx context.Context, config *Config) error {

    return nil
}

func backupFile(ctx context.Context, configPath string, cfg *Config) error {
    data, err := os.ReadFile(configPath)
    if err != nil {
        logutil.Errorf("read file %s failed, err: %v", configPath, err)
        //!!!neglect the error
        return nil
    }
    uid, _ := uuid.NewUUID()
    _, file := path.Split(configPath)
    filename := configDir + "/" + file + "_" + uid.String()
    return writeFile(ctx, cfg.GeneralDir, filename, data)
}