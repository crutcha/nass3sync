package main

import "github.com/aws/aws-sdk-go-v2/service/sns"

type MockSNSClient struct {
	PublishRequests []*sns.PublishInput
}

func (c *MockSNSClient) PublishMessage(msg *sns.PublishInput) error {
	c.PublishRequests = append(c.PublishRequests, msg)
	return nil
}

func NewMockSNSClient() *MockSNSClient {
	return &MockSNSClient{
		PublishRequests: make([]*sns.PublishInput, 0),
	}
}
