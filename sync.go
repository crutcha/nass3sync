package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	// TODO: is there some better way to allow for stubbing filesystem interactions for tests?
	concreteWalkFunc = walkDirectory
)

type ObjectRequests struct {
	TombstoneKeys []string
	DeleteKeys    []string
	UploadKeys    map[string]string
}

type ResultMap struct {
	Upload    map[string]error
	Tombstone map[string]error
	Delete    map[string]error
	lock      *sync.Mutex
}

func (r *ResultMap) AddUploadResult(key string, result error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Upload[key] = result
}

func (r *ResultMap) AddTombstoneResult(key string, result error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Tombstone[key] = result
}

func (r *ResultMap) AddDeleteResult(key string, result error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Delete[key] = result
}

func doSync(client BucketClient, sc SyncConfig, notifier Notifier, lock *sync.Mutex) (*ResultMap, error) {
	resultMap := &ResultMap{
		Upload:    make(map[string]error),
		Delete:    make(map[string]error),
		Tombstone: make(map[string]error),
		lock:      new(sync.Mutex),
	}
	if !lock.TryLock() {
		log.Warn("Another sync routine is already running. Skipping.")
		return resultMap, fmt.Errorf("Unable to acquire sync lock")
	}
	defer lock.Unlock()
	log.Info(fmt.Sprintf("Sync starting for %s.", sc.SourceFolder))
	syncStartTime := time.Now()

	// TODO: for now with a small number of exclusion matchers, this OK, but we should figure out
	// a more efficient way to do this to handle a larger amount of exception patterns
	regexStr := strings.Join(sc.Exclude, "|")
	exclude := regexp.MustCompile(regexStr)

	objectRequests := ObjectRequests{
		TombstoneKeys: make([]string, 0),
		DeleteKeys:    make([]string, 0),
		UploadKeys:    make(map[string]string),
	}

	bucketFiles, listBucketErr := client.ListObjects(sc.DestinationBucket)
	if listBucketErr != nil {
		log.Warn(fmt.Sprintf("listBucket err: %s", listBucketErr))
		return resultMap, fmt.Errorf("Error listing S3 bucket: %s", listBucketErr)
	}
	localFiles, listLocalFilesErr := concreteWalkFunc(sc.SourceFolder)
	if listLocalFilesErr != nil {
		log.Warn(fmt.Sprintf("listLocalFilesErr: %s", listLocalFilesErr))
		return resultMap, fmt.Errorf("Error walking local directory: %s", listLocalFilesErr)
	}

	for localPath, localFileInfo := range localFiles {
		isExcluded := len(sc.Exclude) != 0 && exclude.MatchString(localPath)
		if isExcluded {
			log.Info(fmt.Sprintf("%s matches exclusion list. skipping...", localPath))
			continue
		}

		pathComponents := strings.Split(localPath, sc.SourceFolder)
		uploadKey := pathComponents[1]
		remoteObj, ok := bucketFiles[strings.TrimPrefix(uploadKey, "/")]

		// S3 will apply it's own last modified timestamp when an object is uploaded, the timestamp from local file
		// stat wont match. As long as the last modified timestamp from S3 for any given file/key combo is more recent
		// than the local file last modified timestamp, S3 has the most recent copy. we could use our own metadata
		// to track local file modification time, but this would require a HeadObject call for every file, and on
		// a large drive/bucket, that's a ton of API calls which both slow this down considerably and cost more.
		if !ok {
			objectRequests.UploadKeys[uploadKey] = localPath
		} else {
			localFileSize := localFileInfo.Size()
			timeSinceUpdate := remoteObj.ModTime.Sub(localFileInfo.ModTime())
			if timeSinceUpdate < 0 || localFileSize != remoteObj.Size {
				log.Info(fmt.Sprintf("%s has been modified, will update", localPath))
				objectRequests.UploadKeys[uploadKey] = localPath
			} else {
				log.Debug(fmt.Sprintf("%s is in sync, no action required", localPath))
			}
		}
	}

	for key, _ := range bucketFiles {
		if !strings.HasPrefix(key, "/") {
			key = "/" + key
		}
		localPathPrefix := strings.TrimSuffix(sc.SourceFolder, "/")
		localPathForKey := localPathPrefix + key
		_, ok := localFiles[localPathForKey]
		if !ok && sc.Destructive {
			if sc.TombstoneBucket != "" {
				objectRequests.TombstoneKeys = append(objectRequests.TombstoneKeys, key)
			} else {
				objectRequests.DeleteKeys = append(objectRequests.DeleteKeys, key)
			}
		}
	}

	syncObjectRequests(client, objectRequests, resultMap, sc.DestinationBucket, sc.TombstoneBucket)
	syncEndTime := time.Now()
	duration := syncEndTime.Sub(syncStartTime)
	log.Info(fmt.Sprintf("Sync complete for %s. Took %s", sc.SourceFolder, duration.String()))

	if notifier != nil {
		notifier.NotifySyncResults(sc, resultMap)
	}

	return resultMap, nil
}

func syncObjectRequests(client BucketClient, objReqs ObjectRequests, resultMap *ResultMap, destBucket, tombstoneBucket string) {
	var wg sync.WaitGroup

	for fileKey, fileInfo := range objReqs.UploadKeys {
		wg.Add(1)
		go doUploadFile(client, destBucket, fileKey, fileInfo, &wg, resultMap)
	}

	if tombstoneBucket != "" {
		for _, key := range objReqs.TombstoneKeys {
			wg.Add(1)
			go doTombstoneObject(client, destBucket, tombstoneBucket, key, &wg, resultMap)
		}
	}

	for _, key := range objReqs.DeleteKeys {
		wg.Add(1)
		go doDeleteObject(client, destBucket, key, &wg, resultMap)
	}

	wg.Wait()
}

func doUploadFile(
	client BucketClient,
	bucket, key, filePath string,
	wg *sync.WaitGroup,
	resultMap *ResultMap,
) error {
	resultMap.AddUploadResult(key, nil)
	semaphore <- 1
	defer wg.Done()

	fd, fileErr := os.Open(filePath)
	if fileErr != nil {
		resultMap.AddUploadResult(key, fileErr)
		<-semaphore
		return fileErr
	}
	defer fd.Close()

	key = strings.TrimPrefix(key, "/")
	uploadErr := client.UploadFile(bucket, key, fd)
	if uploadErr != nil {
		resultMap.AddUploadResult(key, uploadErr)
	} else {
		log.Info(fmt.Sprintf("Uploaded file %s as key %s", filePath, key))
	}
	<-semaphore

	return uploadErr
}

func doTombstoneObject(
	client BucketClient,
	sourceBucket, destinationBucket, key string,
	wg *sync.WaitGroup,
	resultMap *ResultMap,
) error {
	resultMap.AddTombstoneResult(key, nil)
	semaphore <- 1
	defer wg.Done()

	copyErr := client.CopyObject(sourceBucket, destinationBucket, key)
	log.Info(fmt.Sprintf("Copied %s from %s to %s", key, sourceBucket, destinationBucket))

	if copyErr != nil {
		log.Warn(fmt.Sprintf("Error copying object during tombstone routine: %s", copyErr))
		resultMap.AddTombstoneResult(key, copyErr)
		<-semaphore
		return copyErr
	}

	delErr := client.DeleteObject(sourceBucket, key)

	if delErr != nil {
		log.Warn(fmt.Sprintf("Error deleting original object during tombstone routine: %s", delErr))
		resultMap.AddTombstoneResult(key, delErr)
		<-semaphore
		return delErr
	} else {
		log.Info(fmt.Sprintf("Deleted %s from bucket %s", key, sourceBucket))
	}

	<-semaphore
	return nil
}

func doDeleteObject(
	client BucketClient,
	bucket, key string,
	wg *sync.WaitGroup,
	resultMap *ResultMap,
) error {
	resultMap.AddDeleteResult(key, nil)
	defer wg.Done()
	delErr := client.DeleteObject(bucket, key)

	if delErr != nil {
		log.Warn(fmt.Sprintf("Error deleting: %s", delErr))
		resultMap.AddDeleteResult(key, delErr)
		<-semaphore
		return delErr
	} else {
		log.Info(fmt.Sprintf("Deleted %s from bucket %s", key, bucket))
	}
	return nil
}

func doBackup(client BucketClient, bc BackupConfig, notifier Notifier) {
	fileMap, walkErr := concreteWalkFunc(bc.SourceFolder)
	if walkErr != nil {
		log.Error(fmt.Sprintf("Backup directory walk failed: %s", walkErr))

	}

	filesToCompress := make([]string, 0)
	for key, _ := range fileMap {
		filesToCompress = append(filesToCompress, key)
	}

	now := time.Now()
	backupTimestamp := now.Format(time.RFC3339)
	keyBase := strings.ReplaceAll(bc.SourceFolder, "/", "_")
	backupPrefix := fmt.Sprintf("%s_%s_*.tar.gz", strings.TrimPrefix(keyBase, "_"), backupTimestamp)
	tarFile, _ := ioutil.TempFile(os.TempDir(), backupPrefix)
	defer os.Remove(tarFile.Name())

	log.Info(fmt.Sprintf("Creating backup tarball: %s", tarFile.Name()))
	createArchive(filesToCompress, tarFile)

	// this is janky but the way this is written, this file descripter would be closed already
	// so....we need to open the file again
	uploadFile, uploadFileOpenErr := os.Open(tarFile.Name())
	if uploadFileOpenErr != nil {
		log.Warn("Error uploading backup: ", uploadFileOpenErr)
		return
	}
	defer uploadFile.Close()

	fileKey := filepath.Base(tarFile.Name())
	putErr := client.UploadFile(bc.DestinationBucket, fileKey, uploadFile)
	if putErr != nil {
		log.Warn("Backup upload error: ", putErr)
	} else {
		log.Info("Upload succeded for ", fileKey)
	}

	if notifier != nil {
		notifier.NotifyBackupResults(bc, tarFile, putErr)
	}
}
