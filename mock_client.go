package main

import (
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type MockS3Client struct {
	UploadRequests []MockRequest
	CopyRequests   []MockRequest
	DeleteRequests []MockRequest
	mockList       []types.Object
}

type MockRequest struct {
	SourceBucket string
	DestBucket   string
	Key          string
}

func NewMockClient(mockedList []types.Object) *MockS3Client {
	return &MockS3Client{
		UploadRequests: make([]MockRequest, 0),
		mockList:       mockedList,
	}
}

func (s *MockS3Client) UploadFile(bucketName string, key string, file *os.File) error {
	s.UploadRequests = append(s.UploadRequests, MockRequest{DestBucket: bucketName, Key: key})
	return nil
}

func (s *MockS3Client) ListObjects(string) (map[string]types.Object, error) {
	mockListMap := make(map[string]types.Object)
	for _, obj := range s.mockList {
		mockListMap[*obj.Key] = obj
	}

	return mockListMap, nil
}

func (s *MockS3Client) CopyObject(sourceBucket string, destinationBucket string, key string) error {
	request := MockRequest{SourceBucket: sourceBucket, DestBucket: destinationBucket, Key: key}
	s.CopyRequests = append(s.CopyRequests, request)
	return nil
}
func (s *MockS3Client) DeleteObject(bucket string, key string) error {
	request := MockRequest{DestBucket: bucket, Key: key}
	s.DeleteRequests = append(s.CopyRequests, request)
	return nil
}
