package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"os"
	"path/filepath"
)

type SyncHandler struct {
	bucketFiles map[string]types.Object
	localFiles  map[string]os.FileInfo
	s3Client    *s3.Client
	appConfig   AppConfig
}

func NewSyncHandler(s3Client *s3.Client, appConfig AppConfig) *SyncHandler {
	bucketFiles := make(map[string]types.Object)
	localFiles := make(map[string]os.FileInfo)
	return &SyncHandler{
		bucketFiles: bucketFiles,
		localFiles:  localFiles,
		s3Client:    s3Client,
		appConfig:   appConfig,
	}
}

func (s *SyncHandler) gatherS3Objects() error {
	listParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.appConfig.DestinationBucket),
	}
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, listParams, func(o *s3.ListObjectsV2PaginatorOptions) {})
	for paginator.HasMorePages() {
		currentPage, pageErr := paginator.NextPage(context.TODO())
		if pageErr != nil {
			return pageErr
		}
		for _, object := range currentPage.Contents {
			s.bucketFiles[*object.Key] = object
		}
	}

	return nil
}

func (s *SyncHandler) gatherLocalFiles() error {
	walkErr := filepath.Walk(s.appConfig.SourceFolder, func(path string, f os.FileInfo, err error) error {
		s.localFiles[path] = f
		return nil
	})
	if walkErr != nil {
		return walkErr
	}

	return nil
}

func (s *SyncHandler) Sync() error {
	return nil
}
