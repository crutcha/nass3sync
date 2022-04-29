package main

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/assert"
)

func createMockWalkFunc(mockResult map[string]os.FileInfo) walkFunc {
	return func(string) (map[string]os.FileInfo, error) {
		return mockResult, nil
	}
}

func TestTheSyncHandler(t *testing.T) {
	assert.True(t, true)
	now := time.Now()

	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now(),
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := []types.Object{
		types.Object{
			ChecksumAlgorithm: []types.ChecksumAlgorithm{},
			ETag:              aws.String("mock-etag"),
			Key:               aws.String("mock-key"),
			LastModified:      &now,
			Owner:             &types.Owner{},
			Size:              1,
			//StorageClass:      types.ObjectStorageClass{},
		},
	}
	mockS3Client := NewMockClient(mockBucketList)
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
	}

	syncHandler := NewSyncHandler(mockS3Client, &sns.Client{}, mockSyncConfig, "")
	syncedObjects, syncErr := syncHandler.Sync()

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.TombstoneKeys, 0)
	assert.Len(t, syncedObjects.UploadKeys, 1)
	assert.Contains(t, syncedObjects.UploadKeys, "/folder2/not-real-file")
}
