package main

import (
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
