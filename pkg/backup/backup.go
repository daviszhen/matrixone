package backup

import (
    "context"
)

// Backup
// Note: ctx needs to support cancel. The user can cancel the backup task by canceling the ctx.
func Backup(ctx context.Context, cfg *BackupConfig) error {
    var err error

    // step 1 : setup fileservice

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
func backupBuildInfo(ctx context.Context, cfg *BackupConfig) error {

    return nil
}

// saveConfigs saves cluster config or service config
func backupConfigs(ctx context.Context, cfg *BackupConfig) error {

    return nil
}

func backupTae(ctx context.Context, config *BackupConfig) error {

    return nil
}

func backupHakeeper(ctx context.Context, config *BackupConfig) error {

    return nil
}
