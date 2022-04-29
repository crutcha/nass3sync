package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
)

var mockedList []types.Object

func TestTarAndUploadSimple(t *testing.T) {
	mockTempDir, mockTempDirErr := ioutil.TempDir(os.TempDir(), "go-test-stuff")
	assert.Nil(t, mockTempDirErr)
	defer os.RemoveAll(mockTempDir)

	_, tempFileErr := os.CreateTemp(mockTempDir, "fake-test-file")
	assert.Nil(t, tempFileErr)

	mockBackupConfig := BackupConfig{
		SourceFolder:      mockTempDir,
		DestinationBucket: "notatallarealbucket",
		At:                "*/1 * * * *",
	}
	mockClient := NewMockClient(mockedList)
	keyBase := strings.TrimPrefix(strings.ReplaceAll(mockTempDir, "/", "_"), "_")
	keyRegex := fmt.Sprintf("^%s.*\\.tar\\.gz$", keyBase)

	tarAndUploadBackup(mockBackupConfig, mockClient)

	assert.Len(t, mockClient.Requests, 1)
	assert.Equal(t, *mockClient.Requests[0].Bucket, "notatallarealbucket")
	assert.Regexp(t, regexp.MustCompile(keyRegex), *mockClient.Requests[0].Key)
}

func TestTarAndUploadNested(t *testing.T) {
	mockTempDir, mockTempDirErr := ioutil.TempDir(os.TempDir(), "go-test-stuff")
	assert.Nil(t, mockTempDirErr)
	defer os.RemoveAll(mockTempDir)

	nestedRelativeDir := "one/two/three"
	nestedAbsoluteDir := fmt.Sprintf("%s/%s", mockTempDir, nestedRelativeDir)
	mkdirAllErr := os.MkdirAll(nestedAbsoluteDir, os.ModePerm)
	assert.Nil(t, mkdirAllErr)

	_, tempFileErr := os.CreateTemp(nestedAbsoluteDir, "fake-test-file")
	assert.Nil(t, tempFileErr)

	mockBackupConfig := BackupConfig{
		SourceFolder:      nestedAbsoluteDir,
		DestinationBucket: "notatallarealbucket",
		At:                "*/1 * * * *",
	}
	mockClient := NewMockClient(mockedList)
	keyBase := strings.TrimPrefix(strings.ReplaceAll(mockTempDir, "/", "_"), "_")
	keyRegex := fmt.Sprintf("^%s.*\\.tar\\.gz$", keyBase)

	tarAndUploadBackup(mockBackupConfig, mockClient)
	assert.Len(t, mockClient.Requests, 1)
	assert.Equal(t, *mockClient.Requests[0].Bucket, "notatallarealbucket")
	assert.Regexp(t, regexp.MustCompile(keyRegex), *mockClient.Requests[0].Key)
}
