package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type ObjectRequests struct {
	TombstoneKeys []string
	UploadKeys    map[string]string
}

type SyncResults struct {
	Key    string
	Result string
}

type SyncHandler struct {
	bucketFiles map[string]types.Object
	localFiles  map[string]os.FileInfo
	s3Client    *s3.Client
	syncConfig  SyncConfig
	mutex       sync.Mutex
}

func NewSyncHandler(s3Client *s3.Client, syncConfig SyncConfig) *SyncHandler {
	bucketFiles := make(map[string]types.Object)
	localFiles := make(map[string]os.FileInfo)
	return &SyncHandler{
		bucketFiles: bucketFiles,
		localFiles:  localFiles,
		s3Client:    s3Client,
		syncConfig:  syncConfig,
	}
}

func (s *SyncHandler) gatherS3Objects() error {
	log.Info(fmt.Sprintf("Gathering S3 objects to compare from bucket %s\n", s.syncConfig.DestinationBucket))
	listParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.syncConfig.DestinationBucket),
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
	log.Info(fmt.Sprintf("Gathering local files recursively for directory %s\n", s.syncConfig.SourceFolder))
	walkErr := filepath.Walk(s.syncConfig.SourceFolder, func(path string, f os.FileInfo, err error) error {
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
	if !s.mutex.TryLock() {
		log.Info("Another sync routine is already running. Skipping.")
		return nil
	}
	defer s.mutex.Unlock()

	log.Info(fmt.Sprintf("Starting sync routine for %s", s.syncConfig.SourceFolder))
	syncStartTime := time.Now()
	s3GatherErr := s.gatherS3Objects()
	if s3GatherErr != nil {
		return fmt.Errorf("s3gather error: %s", s3GatherErr)
	}

	localGatherErr := s.gatherLocalFiles()
	if localGatherErr != nil {
		return fmt.Errorf("localgather error: %s", localGatherErr)
	}

	//var objectRequests ObjectRequests
	objectRequests := ObjectRequests{
		TombstoneKeys: make([]string, 0),
		UploadKeys:    make(map[string]string),
	}
	for localPath, localFileInfo := range s.localFiles {
		pathComponents := strings.Split(localPath, s.syncConfig.SourceFolder)
		// TODO: ensure we have at least 2 components?
		uploadKey := pathComponents[1]
		val, ok := s.bucketFiles[localPath]

		// TODO: do we need to worry about timezones or anything like that?
		if !ok {
			objectRequests.UploadKeys[uploadKey] = localPath
			//log.Debug(fmt.Sprintf("%s does not exist in bucket, will upload with key %s", localPath, uploadKey))
		} else if ok && *val.LastModified != localFileInfo.ModTime() {
			objectRequests.UploadKeys[uploadKey] = localPath
			//log.Info(fmt.Sprintf("%s has been modified, will update", localPath))
		} else {
			log.Info(fmt.Sprintf("%s is in sync, no action required", localPath))
		}
	}

	for key, _ := range s.bucketFiles {
		_, ok := s.localFiles[key]
		if !ok {
			objectRequests.TombstoneKeys = append(objectRequests.TombstoneKeys, key)
			//log.Info(fmt.Sprintf("%s exists in bucket but not locally, will tombstone", key))
		}
	}

	s.syncObjectRequests(objectRequests)
	syncEndTime := time.Now()
	duration := syncEndTime.Sub(syncStartTime)
	log.Info("Sync complete. Took %s\n", duration.String())

	return nil
}

func (s *SyncHandler) syncObjectRequests(objReqs ObjectRequests) {
	// TODO: from app config
	semaphore := make(chan int, 5)
	var wg sync.WaitGroup
	reportStr := "Uploads:\n"

	for fileKey, fileInfo := range objReqs.UploadKeys {
		wg.Add(1)
		go s.uploadFile(fileKey, fileInfo, semaphore, wg)
		//reportStr += fmt.Sprintf("    Key: %s  Result: %s\n", fileKey, "SUCCESS")
	}
	reportStr += "\n\n\nTombstones: \n"
	for _, key := range objReqs.TombstoneKeys {
		fmt.Sprintf(key)
		//reportStr += fmt.Sprintf("    Key: %s  Result: %s\n", key, "SUCCESS")
	}

	wg.Wait()
	fmt.Println(reportStr)
}

func (s *SyncHandler) uploadFile(key, filePath string, semaphore chan int, wg sync.WaitGroup) error {
	semaphore <- 1
	defer wg.Done()

	fd, fileErr := os.Open(filePath)
	if fileErr != nil {
		return fileErr
	}
	defer fd.Close()

	log.Info(fmt.Sprintf("Uploading file %s as key %s\n", filePath, key))
	uploader := manager.NewUploader(s.s3Client)
	_, putErr := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.syncConfig.DestinationBucket),
		Key:    aws.String(strings.TrimPrefix(key, "/")),
		Body:   fd,
	})
	<-semaphore

	return putErr
}
