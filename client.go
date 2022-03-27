package main

import (
	"os"
	"time"
)

type BucketClient interface {
	ListObjects(string) (map[string]ObjectInfo, error)
	UploadFile(bucketName string, key string, file *os.File) error
	CopyObject(sourceBucket string, destinationBucket string, key string) error
	DeleteObject(bucket string, key string) error
}

type ObjectInfo struct {
	ModTime time.Time
	Size    int64
}
