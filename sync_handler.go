package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	log "github.com/sirupsen/logrus"
	"net/url"
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
	snsClient   *sns.Client
	syncConfig  SyncConfig
	mutex       sync.Mutex
	snsTopic    string
}

func NewSyncHandler(s3Client *s3.Client, snsClient *sns.Client, syncConfig SyncConfig, snsTopic string) *SyncHandler {
	bucketFiles := make(map[string]types.Object)
	localFiles := make(map[string]os.FileInfo)
	return &SyncHandler{
		bucketFiles: bucketFiles,
		localFiles:  localFiles,
		s3Client:    s3Client,
		snsClient:   snsClient,
		syncConfig:  syncConfig,
		snsTopic:    snsTopic,
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
		_, ok := s.bucketFiles[strings.TrimPrefix(uploadKey, "/")]

		// TODO: do we need to worry about timezones or anything like that?
		if !ok {
			objectRequests.UploadKeys[uploadKey] = localPath
			//log.Debug(fmt.Sprintf("%s does not exist in bucket, will upload with key %s", localPath, uploadKey))
			//} else if ok && *val.LastModified != localFileLastModified {
		} else {
			headObjReq := &s3.HeadObjectInput{
				Bucket: aws.String(s.syncConfig.DestinationBucket),
				Key:    aws.String(strings.TrimPrefix(uploadKey, "/")),
			}
			headObjResult, headObjErr := s.s3Client.HeadObject(context.TODO(), headObjReq)
			if headObjErr != nil {
				log.Warn(fmt.Sprintf("Error with head object request to grab metadata for %s: %s", uploadKey, headObjErr))
			}
			// TODO: error check non-existant key?
			localFileLastModified := headObjResult.Metadata["localfilelastmodified"]
			if localFileInfo.ModTime().String() != localFileLastModified {
				log.Info(fmt.Sprintf("%s has been modified, will update", localPath))
				objectRequests.UploadKeys[uploadKey] = localPath
			} else {
				log.Debug(fmt.Sprintf("%s is in sync, no action required", localPath))
			}
		}
	}

	for key, _ := range s.bucketFiles {
		if !strings.HasPrefix(key, "/") {
			key = "/" + key
		}
		localPathPrefix := strings.TrimSuffix(s.syncConfig.SourceFolder, "/")
		localPathForKey := localPathPrefix + key
		_, ok := s.localFiles[localPathForKey]
		if !ok {
			objectRequests.TombstoneKeys = append(objectRequests.TombstoneKeys, key)
			log.Info(fmt.Sprintf("%s exists in bucket but not locally, will tombstone", key))
		}
	}

	s.syncObjectRequests(objectRequests)
	syncEndTime := time.Now()
	duration := syncEndTime.Sub(syncStartTime)
	log.Info(fmt.Sprintf("Sync complete. Took %s\n", duration.String()))

	if s.snsTopic != "" {
		notifyErr := notifySyncResultsViaSns(s.snsClient, s.snsTopic, objectRequests)
		if notifyErr != nil {
			log.Warn(fmt.Sprintf("Error notifying sync results: %s", notifyErr))
		}
	}

	return nil
}

func (s *SyncHandler) syncObjectRequests(objReqs ObjectRequests) {
	// TODO: from app config
	semaphore := make(chan int, 5)
	var wg sync.WaitGroup

	for fileKey, fileInfo := range objReqs.UploadKeys {
		wg.Add(1)
		go s.uploadFile(fileKey, fileInfo, semaphore, &wg)
	}

	if s.syncConfig.TombstoneBucket != "" {
		for _, key := range objReqs.TombstoneKeys {
			wg.Add(1)
			go s.tombstoneObject(key, semaphore, &wg)
		}

	}

	fmt.Println("WAITING ON WAITGROUP")
	wg.Wait()
	fmt.Println("AFTER WAITGROUP")
}

func (s *SyncHandler) uploadFile(key, filePath string, semaphore chan int, wg *sync.WaitGroup) error {
	semaphore <- 1
	defer wg.Done()

	fd, fileErr := os.Open(filePath)
	if fileErr != nil {
		<-semaphore
		return fileErr
	}
	defer fd.Close()

	// The last modified timestamp will not match what the local filesystem has. We cant depened solely on file
	// size to determine if a file has changed and needs to be synced again since something like photo rotation
	// would alter the photo but the filesize would remain unchanged. We will track the local file modified time
	// in custom metadata so we can detect cases like this.
	localFileStat, _ := fd.Stat()
	customMetadata := map[string]string{"localfilelastmodified": localFileStat.ModTime().String()}

	log.Info(fmt.Sprintf("Uploading file %s as key %s\n", filePath, key))
	uploader := manager.NewUploader(s.s3Client)
	_, putErr := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:   aws.String(s.syncConfig.DestinationBucket),
		Key:      aws.String(strings.TrimPrefix(key, "/")),
		Body:     fd,
		Metadata: customMetadata,
	})
	<-semaphore

	return putErr
}

func (s *SyncHandler) tombstoneObject(key string, semaphore chan int, wg *sync.WaitGroup) error {
	semaphore <- 1
	defer wg.Done()

	/*
		copyReq := &s3.CopyObjectInput{
			CopySource: aws.String(s.syncConfig.TombstoneBucket),
			Bucket:     aws.String(url.PathEscape(s.syncConfig.DestinationBucket)),
			Key:        aws.String(strings.TrimPrefix(key, "/")),
		}
	*/
	source := s.syncConfig.DestinationBucket + "/" + strings.TrimPrefix(key, "/")
	copyReq := &s3.CopyObjectInput{
		Bucket:     aws.String(s.syncConfig.TombstoneBucket),
		CopySource: aws.String(url.PathEscape(source)),
		Key:        aws.String(strings.TrimPrefix(key, "/")),
	}

	_, copyErr := s.s3Client.CopyObject(context.TODO(), copyReq)

	// TODO: for some reason, if we hit this error condition, wg.Wait() never returns
	if copyErr != nil {
		log.Warn(fmt.Sprintf("Error copying object during tombstone routine: %s", copyErr))
		<-semaphore
		return copyErr
	}

	delReq := &s3.DeleteObjectInput{
		Bucket: aws.String(s.syncConfig.DestinationBucket),
		Key:    aws.String(strings.TrimPrefix(key, "/")),
	}

	_, delErr := s.s3Client.DeleteObject(context.TODO(), delReq)

	if delErr != nil {
		log.Warn(fmt.Sprintf("Error deleting original object during tombstone routine: %s", delErr))
		<-semaphore
		return delErr
	}

	<-semaphore
	return nil
}

// TODO: this doesn't actually capture if upload or tombstone operations were successful or
// returned an error. need to update this func to accomodate for each individual key result.
func notifySyncResultsViaSns(snsClient *sns.Client, snsTopic string, objectRequests ObjectRequests) error {
	// we only want to notify if something actually happened
	if len(objectRequests.TombstoneKeys) == 0 && len(objectRequests.UploadKeys) == 0 {
		return nil
	}

	notificationBody := "Uploads:\n"
	for _, upload := range objectRequests.UploadKeys {
		notificationBody += fmt.Sprintf("  - %s\n", upload)
	}

	notificationBody += "\n\nTombstones:\n"
	for _, tombstone := range objectRequests.TombstoneKeys {
		notificationBody += fmt.Sprintf("  - %s\n", tombstone)
	}

	snsPublishReq := &sns.PublishInput{
		Message:  aws.String(notificationBody),
		TopicArn: aws.String(snsTopic),
	}
	_, publishErr := snsClient.Publish(context.TODO(), snsPublishReq)

	return publishErr
}
