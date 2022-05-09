package main

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createMockWalkFunc(mockResult map[string]os.FileInfo) walkFunc {
	return func(string) (map[string]os.FileInfo, error) {
		return mockResult, nil
	}
}

func TestMain(m *testing.M) {
	// semaphore is created by on config init function
	// keep it at 1 for tests
	semaphore = make(chan int, 1)
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestLocalFileNotInBucket(t *testing.T) {
	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now(),
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockS3Client := NewMockClient(map[string]ObjectInfo{})
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Delete, 0)
	assert.Len(t, syncedObjects.Upload, 1)
	assert.Contains(t, syncedObjects.Upload, "/folder2/not-real-file")
}

func TestLocalFileIsOlder(t *testing.T) {
	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			timestamp: time.Now().Add(-1 * time.Hour),
			size:      1,
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := map[string]ObjectInfo{
		"folder2/not-real-file": {
			ModTime: time.Now(),
			Size:    1,
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
	assert.Len(t, syncedObjects.Delete, 0)
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
	mockBucketList := map[string]ObjectInfo{
		"folder2/not-real-life": {
			ModTime: oneHourAgo,
			Size:    1,
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
	assert.Len(t, syncedObjects.Delete, 0)
	assert.Len(t, syncedObjects.Upload, 1)
	assert.Contains(t, syncedObjects.Upload, "/folder2/not-real-file")
}

func TestBucketFileNotOnLocalFSDestructiveWithTombstone(t *testing.T) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	mockFileInfoResults := make(map[string]os.FileInfo)
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := map[string]ObjectInfo{
		"folder2/not-real-file": {
			ModTime: oneHourAgo,
			Size:    1,
		},
	}
	mockS3Client := NewMockClient(mockBucketList)
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
		TombstoneBucket:   "some-tombstone-bucket",
		Destructive:       true,
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 1)
	assert.Len(t, syncedObjects.Upload, 0)
	assert.Contains(t, syncedObjects.Tombstone, "/folder2/not-real-file")
}

func TestBucketFileNotOnLocalFSDestructiveWithoutTombstone(t *testing.T) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	mockFileInfoResults := make(map[string]os.FileInfo)
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := map[string]ObjectInfo{
		"folder2/not-real-file": {
			ModTime: oneHourAgo,
			Size:    1,
		},
	}
	mockS3Client := NewMockClient(mockBucketList)
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
		Destructive:       true,
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Upload, 0)
	assert.Len(t, syncedObjects.Delete, 1)
	assert.Contains(t, syncedObjects.Delete, "/folder2/not-real-file")
}

func TestBucketFileNotOnLocalFSNonDestructive(t *testing.T) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	mockFileInfoResults := make(map[string]os.FileInfo)
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := map[string]ObjectInfo{
		"folder2/not-real-file": {
			ModTime: oneHourAgo,
			Size:    1,
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
	assert.Len(t, syncedObjects.Delete, 0)
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
	mockS3Client := NewMockClient(map[string]ObjectInfo{})
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
		Exclude:           []string{"/folder1/.*/not-real-file"},
	}

	lock := &sync.Mutex{}
	syncedObjects, syncErr := doSync(mockS3Client, mockSyncConfig, nil, lock)

	assert.Nil(t, syncErr)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Delete, 0)
	assert.Len(t, syncedObjects.Upload, 1)
	assert.Contains(t, syncedObjects.Upload, "/folder2/somewhat-real-file")
	assert.NotContains(t, syncedObjects.Upload, "/folder2/not-real-file")
}

func TestSyncRoutineErrosWhenAnotherIsRunning(t *testing.T) {
	mockFileInfoResults := make(map[string]os.FileInfo)
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockS3Client := NewMockClient(map[string]ObjectInfo{})
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
	assert.Len(t, syncedObjects.Delete, 0)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Len(t, syncedObjects.Upload, 0)
}

func TestSameModTimeFileSizeDifferent(t *testing.T) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	mockFileInfoResults := map[string]os.FileInfo{
		"/folder1/folder2/not-real-file": mockFileInfo{
			isDir:     false,
			size:      10,
			timestamp: oneHourAgo,
		},
	}
	concreteWalkFunc = createMockWalkFunc(mockFileInfoResults)
	mockBucketList := map[string]ObjectInfo{
		"folder2/not-real-life": {
			ModTime: oneHourAgo,
			Size:    1,
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
	assert.Len(t, syncedObjects.Upload, 1)
	assert.Len(t, syncedObjects.Delete, 0)
	assert.Len(t, syncedObjects.Tombstone, 0)
	assert.Contains(t, syncedObjects.Upload, "/folder2/not-real-file")
}
