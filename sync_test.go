package main

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
)

func createMockWalkFunc(mockResult map[string]os.FileInfo) walkFunc {
	return func(string) (map[string]os.FileInfo, error) {
		return mockResult, nil
	}
}

func TestLocalFileNotInBucket(t *testing.T) {
	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now(),
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockS3Client := NewMockClient([]types.Object{})
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Upload, 1)
	assert.Contains(t, syncedObjects.Upload, "/folder2/not-real-file")
}

func TestLocalFileIsOlder(t *testing.T) {
	now := time.Now()

	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now().Add(-1 * time.Hour),
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := []types.Object{
		{
			ChecksumAlgorithm: []types.ChecksumAlgorithm{},
			ETag:              aws.String("mock-etag"),
			Key:               aws.String("folder2/not-real-file"),
			LastModified:      &now,
			Owner:             &types.Owner{},
			Size:              1,
		},
	}
	mockS3Client := NewMockClient(mockBucketList)
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Upload, 0)
}

func TestLocalFileIsNewer(t *testing.T) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now(),
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := []types.Object{
		{
			ChecksumAlgorithm: []types.ChecksumAlgorithm{},
			ETag:              aws.String("mock-etag"),
			Key:               aws.String("folder2/not-real-file"),
			LastModified:      &oneHourAgo,
			Owner:             &types.Owner{},
			Size:              1,
		},
	}
	mockS3Client := NewMockClient(mockBucketList)
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Upload, 1)
	assert.Contains(t, syncedObjects.Upload, "/folder2/not-real-file")
}

func TestBucketFileNotOnLocalFS(t *testing.T) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	mockFileInfoResults := make(map[string]os.FileInfo)
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := []types.Object{
		{
			ChecksumAlgorithm: []types.ChecksumAlgorithm{},
			ETag:              aws.String("mock-etag"),
			Key:               aws.String("folder2/not-real-file"),
			LastModified:      &oneHourAgo,
			Owner:             &types.Owner{},
			Size:              1,
		},
	}
	mockS3Client := NewMockClient(mockBucketList)
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 1)
	assert.Len(t, syncedObjects.Upload, 0)
	assert.Contains(t, syncedObjects.Tombstone, "/folder2/not-real-file")
}

func TestFilesMatchingExclusionNotUploaded(t *testing.T) {
	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now(),
		},
		"/folder1/folder2/somewhat-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now(),
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockS3Client := NewMockClient([]types.Object{})
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
		Exclude:           []string{"/folder1/.*/not-real-file"},
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Upload, 1)
	assert.Contains(t, syncedObjects.Upload, "/folder2/somewhat-real-file")
	assert.NotContains(t, syncedObjects.Upload, "/folder2/not-real-file")
}

func TestSyncRoutineErrosWhenAnotherIsRunning(t *testing.T) {
	mockFileInfoResults := make(map[string]os.FileInfo)
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockS3Client := NewMockClient([]types.Object{})
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
		Exclude:           []string{"/folder1/.*/not-real-file"},
	}

	lock := &sync.Mutex{}
	lock.Lock()
	defer lock.Unlock()
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.NotNil(t, syncErr)
	assert.ErrorContains(t, syncErr, "Unable to acquire sync lock")
	assert.Len(t, mockS3Client.UploadRequests, 0)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Upload, 0)
}
