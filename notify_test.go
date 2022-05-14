package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSNSPublishAllTypes(t *testing.T) {
	mockNotifier := &SNSNotifier{
		Client: NewMockSNSClient(),
		Topic:  "mock-topic",
	}
	mockResults := &ResultMap{
		Upload: map[string]error{
			"uploaded-file": nil,
		},
		Tombstone: map[string]error{
			"uploaded-file": nil,
		},
		Delete: map[string]error{
			"uploaded-file": nil,
		},
		lock: new(sync.Mutex),
	}
	mockSyncConfig := SyncConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "not-real-bucket",
	}
	expectedSubject := "Sync results: /folder1 -> not-real-bucket"
	expectedMessage := `Uploads:
  - uploaded-file => <nil>


Tombstones:
  - uploaded-file => <nil>


Deleted:
  - uploaded-file => <nil>
`

	mockNotifier.NotifySyncResults(mockSyncConfig, mockResults)

	mockClient := mockNotifier.Client.(*MockSNSClient)
	assert.Len(t, mockClient.PublishRequests, 1)
	assert.Equal(t, *mockClient.PublishRequests[0].Subject, expectedSubject)
	assert.Equal(t, *mockClient.PublishRequests[0].Message, expectedMessage)
}

func TestSNSNotifyBackupSuccess(t *testing.T) {
	mockNotifier := &SNSNotifier{
		Client: NewMockSNSClient(),
		Topic:  "mock-topic",
	}
	mockBackupConfig := BackupConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "some-bucket",
		At:                "*/1 * * * *",
	}
	mockFile, _ := ioutil.TempFile(os.TempDir(), "some-backup-file")
	defer mockFile.Close()
	expectedSubject := "Backup succeeded: /folder1"
	expectedMessage := fmt.Sprintf(`Backup File Name: %s
Backup File Size: 0
Error: <nil>
`, filepath.Base(mockFile.Name()))

	mockNotifier.NotifyBackupResults(mockBackupConfig, mockFile, nil)

	mockClient := mockNotifier.Client.(*MockSNSClient)
	assert.Len(t, mockClient.PublishRequests, 1)
	assert.Equal(t, *mockClient.PublishRequests[0].Subject, expectedSubject)
	assert.Equal(t, *mockClient.PublishRequests[0].Message, expectedMessage)
}

func TestSNSNotifyBackupFailed(t *testing.T) {
	mockNotifier := &SNSNotifier{
		Client: NewMockSNSClient(),
		Topic:  "mock-topic",
	}
	mockBackupConfig := BackupConfig{
		SourceFolder:      "/folder1",
		DestinationBucket: "some-bucket",
		At:                "*/1 * * * *",
	}
	mockFile, _ := ioutil.TempFile(os.TempDir(), "some-backup-file")
	defer mockFile.Close()
	mockErr := fmt.Errorf("unauthorized")
	expectedSubject := "Backup failed: /folder1"
	expectedMessage := fmt.Sprintf(`Backup File Name: %s
Backup File Size: 0
Error: unauthorized
`, filepath.Base(mockFile.Name()))

	mockNotifier.NotifyBackupResults(mockBackupConfig, mockFile, mockErr)

	mockClient := mockNotifier.Client.(*MockSNSClient)
	assert.Len(t, mockClient.PublishRequests, 1)
	assert.Equal(t, *mockClient.PublishRequests[0].Subject, expectedSubject)
	assert.Equal(t, *mockClient.PublishRequests[0].Message, expectedMessage)
}
