package backup

import (
    "context"
    "fmt"
    "github.com/matrixorigin/matrixone/pkg/pb/task"
    "strings"
    "time"
)

func BackupTaskExecutor() func(ctx context.Context, task task.Task) error {
    execBackupTask := func(ctx context.Context, task task.Task) error {
        params := strings.Split(string(task.Metadata.Context), " ")
        fmt.Println("+++>backup task start", params)
        select {
        case <-ctx.Done():
        case <-time.After(time.Second * 30):
        }
        //CALL DN backup api
        fmt.Println("+++>backup task end")
        return nil
    }

    return execBackupTask
}
