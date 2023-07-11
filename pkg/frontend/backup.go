package frontend

import (
    "context"
    "fmt"
    "github.com/matrixorigin/matrixone/pkg/common/moerr"
    "github.com/matrixorigin/matrixone/pkg/common/runtime"
    "github.com/matrixorigin/matrixone/pkg/pb/task"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
    "github.com/matrixorigin/matrixone/pkg/taskservice"
    "strings"
)

func (mce *MysqlCmdExecutor) handleStartBackup(ctx context.Context, sb *tree.BackupStart) error {
    return doBackup(ctx, mce.GetSession(), sb)
}

func doBackup(ctx context.Context, ses *Session, sb *tree.BackupStart) error {
    fmt.Println("+++>doBackup start", sb.Timestamp, sb.Dir)
    defer func() {
        fmt.Println("+++>doBackup end", sb.Timestamp, sb.Dir)
    }()
    v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.TaskService)
    if !ok {
        return moerr.NewInternalError(ctx, "task service is not ready")
    }

    taskService := v.(taskservice.TaskService)
    if taskService == nil {
        return moerr.NewInternalError(ctx, "task service is nil")
    }
    meta := task.TaskMetadata{
        ID:       "backup-0xabc",
        Executor: task.TaskCode_Backup,
        Context:  []byte(strings.Join([]string{sb.Timestamp, sb.Dir}, " ")),
        Options:  task.TaskOptions{Concurrency: 1},
    }
    return taskService.Create(ctx, meta)
}
