package main

import (
	"sync"
)

var (
	// TODO: is there some better way to allow for stubbing filesystem interactions for tests?
	concreteWalkFunc = walkDirectory
)

type ObjectRequests struct {
	TombstoneKeys []string
	UploadKeys    map[string]string
}

type SyncResults struct {
	Tombstone sync.Map
	Upload    sync.Map
}

type SyncHandler interface {
	Sync(sourceFolder, destBucket, tombstoneBucket string, excludePatterns []string)
	Backup(folder, bucket string)
}
