package main

import (
	"os"
)

type Notifier interface {
	NotifySyncResults(SyncConfig, *ResultMap) error
	NotifyBackupResults(backupConfig BackupConfig, backupFile *os.File, backupErr error) error
}
