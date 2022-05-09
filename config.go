package main

import (
	"fmt"

	"github.com/jinzhu/configor"
)

var (
	bucketClientFactoryMap = map[string]BucketClientFactory{
		"aws": NewS3BucketClient,
		"gcs": NewGCSBucketClient,
	}
	notifierFactoryMap = map[string]NotifierFactory{
		"sns": NewSNSNotifier,
	}
	semaphore chan int
)

func InitAppConfig(filepath string) (AppConfig, error) {
	var appConfig AppConfig
	configErr := configor.Load(&appConfig, filepath)
	if configErr != nil {
		return appConfig, configErr
	}

	semaphore = make(chan int, appConfig.Concurrency)

	return appConfig, nil
}

type AppConfig struct {
	Provider    CloudProviderConfig
	Notify      NotifyConfig
	Concurrency int `default:"1"`
	Sync        []SyncConfig
	Backup      []BackupConfig
	SNSTopic    string
}

type CloudProviderConfig struct {
	Name           string `required:"true"`
	Profile        string
	CredentialFile string
	Region         string `required:"true"`
}

type NotifyConfig struct {
	Service string
	ID      string
	Profile string
	Region  string
}

type SyncConfig struct {
	SourceFolder      string `required:"true"`
	DestinationBucket string `required:"true"`
	TombstoneBucket   string
	Interval          int `required:"true"`
	Exclude           []string
	Destructive       bool `default:"true"`
}

type BackupConfig struct {
	SourceFolder      string `required:"true"`
	DestinationBucket string `required:"true"`
	At                string `required:"true"`
}

type BucketClientFactory func(AppConfig) (BucketClient, error)
type NotifierFactory func(AppConfig) (Notifier, error)

func BucketClientFromConfig(appConfig AppConfig) (BucketClient, error) {
	var bucketClient BucketClient
	clientFactory, ok := bucketClientFactoryMap[appConfig.Provider.Name]
	if !ok {
		return nil, fmt.Errorf("Unknown cloud object storage provider: %s", appConfig.Provider.Name)
	}

	bucketClient, bucketClientErr := clientFactory(appConfig)
	return bucketClient, bucketClientErr
}

func NotifierFromConfig(appConfig AppConfig) (Notifier, error) {
	var notifier Notifier
	var notifierErr error

	notifierFactory, ok := notifierFactoryMap[appConfig.Notify.Service]
	if ok {
		notifier, notifierErr = notifierFactory(appConfig)
	}

	return notifier, notifierErr
}

func (c AppConfig) ConfigStringArray() []string {
	configStrArr := make([]string, 0)
	configStrArr = append(configStrArr, fmt.Sprintf("  - Region: %s", c.Provider.Region))
	configStrArr = append(configStrArr, fmt.Sprintf("  - IAMProfile: %s", c.Provider.Profile))
	configStrArr = append(configStrArr, fmt.Sprintf("  - CredentialFile: %s", c.Provider.CredentialFile))
	configStrArr = append(configStrArr, fmt.Sprintf("  - Concurrent Uploads: %d", c.Concurrency))

	if c.SNSTopic != "" {
		configStrArr = append(configStrArr, fmt.Sprintf("  - SNSTopic: %s", c.SNSTopic))
	}

	configStrArr = append(configStrArr, "Folders To Sync:")
	for _, syncConfig := range c.Sync {
		configStrArr = append(configStrArr, fmt.Sprintf("%+v", syncConfig))
	}

	configStrArr = append(configStrArr, "Folders To Backup:")
	for _, backupConfig := range c.Backup {
		configStrArr = append(configStrArr, fmt.Sprintf("%+v", backupConfig))
	}

	return configStrArr
}
