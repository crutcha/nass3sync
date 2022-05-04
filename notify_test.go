package main

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func TestSNSMaxSize(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile("nass3sync"),
		config.WithRegion("us-east-2"))

	fmt.Println(err)
	snsClient := sns.NewFromConfig(cfg)

	letters := []rune(" abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, 262144) // max sns size
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]

	}

	snsPublishReq := &sns.PublishInput{
		Message:  aws.String(string(b)),
		TopicArn: aws.String("arn:aws:sns:us-east-2:719670394721:nass3sync"),
	}
	result, publishErr := snsClient.Publish(context.TODO(), snsPublishReq)
	fmt.Printf("SNS result: %+v\n", result)
	fmt.Printf("SNS error: %+v\n", publishErr)
}
