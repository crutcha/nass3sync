package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	log "github.com/sirupsen/logrus"
)

var (
	// TODO: is there some better way to allow for stubbing filesystem interactions for tests?
	concreteWalkFunc = walkDirectory
)

type ObjectRequests struct {
	TombstoneKeys []string
	UploadKeys    map[string]string
}

type SyncResults struct {
	Tombstone sync.Map
	Upload    sync.Map
}

type SyncHandler struct {
	bucketFiles   map[string]types.Object
	localFiles    map[string]os.FileInfo
	s3Client      S3ClientHandler
	snsClient     *sns.Client
	syncConfig    SyncConfig
	mutex         sync.Mutex
	snsTopic      string
	exclusionExpr *regexp.Regexp
	hasExclusion  bool
}

func NewSyncHandler(s3Client S3ClientHandler, snsClient *sns.Client, syncConfig SyncConfig, snsTopic string) *SyncHandler {
	bucketFiles := make(map[string]types.Object)
	localFiles := make(map[string]os.FileInfo)

	hasExclusionExpr := len(syncConfig.Exclude) != 0
	regexStr := strings.Join(syncConfig.Exclude, "|")
	exclude, excludeErr := regexp.Compile(regexStr)
	if excludeErr != nil {
		log.Fatalf("Error creating exclusion list: %s", excludeErr)
	}

	return &SyncHandler{
		bucketFiles:   bucketFiles,
		localFiles:    localFiles,
		s3Client:      s3Client,
		snsClient:     snsClient,
		syncConfig:    syncConfig,
		snsTopic:      snsTopic,
		exclusionExpr: exclude,
		hasExclusion:  hasExclusionExpr,
	}
}

func (s *SyncHandler) gatherS3Objects() error {
	log.Info(fmt.Sprintf("Gathering S3 objects to compare from bucket %s\n", s.syncConfig.DestinationBucket))
	bucketFiles, listErr := s.s3Client.ListObjects(s.syncConfig.DestinationBucket)
	if listErr != nil {
		return listErr
	}

	s.bucketFiles = bucketFiles

	return nil
}

func (s *SyncHandler) gatherLocalFiles() error {
	var walkErr error
	log.Info(fmt.Sprintf("Gathering local files recursively for directory %s\n", s.syncConfig.SourceFolder))

	s.localFiles, walkErr = concreteWalkFunc(s.syncConfig.SourceFolder)
	if walkErr != nil {
		return walkErr
	}

	return nil
}

func (s *SyncHandler) Sync() (ObjectRequests, error) {
	if !s.mutex.TryLock() {
		log.Info("Another sync routine is already running. Skipping.")
		return ObjectRequests{}, nil
	}
	defer s.mutex.Unlock()

	// TODO: probably some better way to handle this
	// since gocron is setup with a pointer to a synchandler, we need to flush
	// the state from the last time it ran
	s.bucketFiles = make(map[string]types.Object)
	s.localFiles = make(map[string]os.FileInfo)

	objectRequests := ObjectRequests{
		TombstoneKeys: make([]string, 0),
		UploadKeys:    make(map[string]string),
	}

	log.Info(fmt.Sprintf("Starting sync routine for %s", s.syncConfig.SourceFolder))
	syncStartTime := time.Now()
	s3GatherErr := s.gatherS3Objects()
	if s3GatherErr != nil {
		return objectRequests, fmt.Errorf("s3gather error: %s", s3GatherErr)
	}

	localGatherErr := s.gatherLocalFiles()
	if localGatherErr != nil {
		return objectRequests, fmt.Errorf("localgather error: %s", localGatherErr)
	}

	for localPath, localFileInfo := range s.localFiles {
		if strings.Contains(localPath, "sessions.json") {
			fmt.Println("debug time")
		}
		isExcluded := s.hasExclusion && s.exclusionExpr.MatchString(localPath)
		if isExcluded {
			log.Info(fmt.Sprintf("%s matches exclusion list. skipping...", localPath))
			continue
		}

		pathComponents := strings.Split(localPath, s.syncConfig.SourceFolder)
		// TODO: ensure we have at least 2 components?
		uploadKey := pathComponents[1]
		remoteObj, ok := s.bucketFiles[strings.TrimPrefix(uploadKey, "/")]

		// S3 will apply it's own last modified timestamp when an object is uploaded, the timestamp from local file
		// stat wont match. As long as the last modified timestamp from S3 for any given file/key combo is more recent
		// than the local file last modified timestamp, S3 has the most recent copy. we could use our own metadata
		// to track local file modification time, but this would require a HeadObject call for every file, and on
		// a large drive/bucket, that's a ton of API calls which both slow this down considerably and cost more.
		if !ok {
			objectRequests.UploadKeys[uploadKey] = localPath
		} else {
			localFileSize := localFileInfo.Size()
			timeSinceUpdate := remoteObj.LastModified.Sub(localFileInfo.ModTime())
			if timeSinceUpdate < 0 || localFileSize != remoteObj.Size {
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

	return objectRequests, nil
}

func (s *SyncHandler) syncObjectRequests(objReqs ObjectRequests) SyncResults {
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

	wg.Wait()
	return SyncResults{}
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

	log.Info(fmt.Sprintf("Uploading file %s as key %s\n", filePath, key))
	key = strings.TrimPrefix(key, "/")
	uploadErr := s.s3Client.UploadFile(s.syncConfig.DestinationBucket, key, fd)
	<-semaphore

	return uploadErr
}

func (s *SyncHandler) tombstoneObject(key string, semaphore chan int, wg *sync.WaitGroup) error {
	semaphore <- 1
	defer wg.Done()

	copyErr := s.s3Client.CopyObject(
		s.syncConfig.DestinationBucket,
		s.syncConfig.TombstoneBucket,
		key,
	)

	if copyErr != nil {
		log.Warn(fmt.Sprintf("Error copying object during tombstone routine: %s", copyErr))
		<-semaphore
		return copyErr
	}

	delErr := s.s3Client.DeleteObject(s.syncConfig.DestinationBucket, key)

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
