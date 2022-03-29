package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
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
	log.Info(fmt.Sprintf("Gathering S3 objects to compare from bucket %s\n", s.appConfig.DestinationBucket))
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
	log.Info(fmt.Sprintf("Gathering local files recursively for directory %s\n", s.appConfig.SourceFolder))
	walkErr := filepath.Walk(s.appConfig.SourceFolder, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			s.localFiles[path] = f
		}
		return nil
	})
	if walkErr != nil {
		return walkErr
	}

	return nil
}

func (s *SyncHandler) Sync() error {
	s3GatherErr := s.gatherS3Objects()
	if s3GatherErr != nil {
		return fmt.Errorf("s3gather error: %s", s3GatherErr)
	}

	localGatherErr := s.gatherLocalFiles()
	if localGatherErr != nil {
		return fmt.Errorf("localgather error: %s", localGatherErr)
	}

	for localPath, localFileInfo := range s.localFiles {
		val, ok := s.bucketFiles[localPath]
		// TODO: do we need to worry about timezones or anything like that?
		if !ok {
			pathComponents := strings.Split(localPath, s.appConfig.SourceFolder)
			// TODO: ensure we have at least 2 components?
			uploadKey := pathComponents[1]
			log.Debug(fmt.Sprintf("%s does not exist in bucket, will upload with key %s", localPath, uploadKey))
		} else if ok && *val.LastModified != localFileInfo.ModTime() {
			log.Info(fmt.Sprintf("%s has been modified, will update", localPath))
		} else {
			log.Info(fmt.Sprintf("%s is in sync, no action required", localPath))
		}
	}

	return nil
}
