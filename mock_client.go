package main

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
)

type MockS3Client struct {
	Requests []*s3.PutObjectInput
}

func NewMockClient() *MockS3Client {
	return &MockS3Client{
		Requests: make([]*s3.PutObjectInput, 0),
	}
}

func (s *MockS3Client) PutObject(request *s3.PutObjectInput, file *os.File) error {
	s.Requests = append(s.Requests, request)
	return nil
}
