package main

import (
	"context"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type BucketClient interface {
	ListObjects(string) (map[string]types.Object, error)
	UploadFile(bucketName string, key string, file *os.File) error
	CopyObject(sourceBucket string, destinationBucket string, key string) error
	DeleteObject(bucket string, key string) error
}

type S3Client struct {
	Client *s3.Client
}

func (s *S3Client) PutObject(putReq *s3.PutObjectInput, file *os.File) error {
	uploader := manager.NewUploader(s.Client)
	_, putErr := uploader.Upload(context.TODO(), putReq)
	return putErr
}

func (s *S3Client) ListObjects(bucketName string) (map[string]types.Object, error) {
	bucketFiles := make(map[string]types.Object)
	listParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	paginator := s3.NewListObjectsV2Paginator(s.Client, listParams, func(o *s3.ListObjectsV2PaginatorOptions) {})
	for paginator.HasMorePages() {
		currentPage, pageErr := paginator.NextPage(context.TODO())
		if pageErr != nil {
			return bucketFiles, pageErr

		}
		for _, object := range currentPage.Contents {
			bucketFiles[*object.Key] = object
		}
	}

	return bucketFiles, nil
}

func (s *S3Client) UploadFile(bucketName, key string, file *os.File) error {
	uploader := manager.NewUploader(s.Client)
	_, putErr := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})

	return putErr
}

func (s *S3Client) CopyObject(sourceBucket, destinationBucket, key string) error {
	source := sourceBucket + "/" + strings.TrimPrefix(key, "/")
	copyReq := &s3.CopyObjectInput{
		Bucket:     aws.String(destinationBucket),
		CopySource: aws.String(url.PathEscape(source)),
		Key:        aws.String(strings.TrimPrefix(key, "/")),
	}
	_, copyErr := s.Client.CopyObject(context.TODO(), copyReq)

	return copyErr
}

func (s *S3Client) DeleteObject(bucket string, key string) error {
	delReq := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(strings.TrimPrefix(key, "/")),
	}
	_, delErr := s.Client.DeleteObject(context.TODO(), delReq)

	return delErr
}
