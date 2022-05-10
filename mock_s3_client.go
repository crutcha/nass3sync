package main

import (
	"os"
)

type MockS3Client struct {
	UploadRequests []MockRequest
	CopyRequests   []MockRequest
	DeleteRequests []MockRequest
	mockList       map[string]ObjectInfo
}

type MockRequest struct {
	SourceBucket string
	DestBucket   string
	Key          string
}

func NewMockClient(mocked map[string]ObjectInfo) *MockS3Client {
	return &MockS3Client{
		UploadRequests: make([]MockRequest, 0),
		mockList:       mocked,
	}
}

func (s *MockS3Client) UploadFile(bucketName string, key string, file *os.File) error {
	s.UploadRequests = append(s.UploadRequests, MockRequest{DestBucket: bucketName, Key: key})
	return nil
}

func (s *MockS3Client) ListObjects(string) (map[string]ObjectInfo, error) {
	return s.mockList, nil
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
