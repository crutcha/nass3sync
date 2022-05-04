package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type AppConfig struct {
	Provider    CloudProviderConfig
	Notify      NotifyConfig
	Concurrency int `default:"1"`
	Sync        []SyncConfig
	Backup      []BackupConfig
	SNSTopic    string
}

type CloudProviderConfig struct {
	Name    string `required:"true"`
	Profile string
	Region  string `required:"true"`
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
}

type BackupConfig struct {
	SourceFolder      string `required:"true"`
	DestinationBucket string `required:"true"`
	At                string `required:"true"`
}

func (c AppConfig) ClientFromConfig() (BucketClient, error) {
	var bucketClient BucketClient

	switch c.Provider.Name {
	case "aws":
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithSharedConfigProfile(c.Provider.Profile),
			config.WithRegion(c.Provider.Region))
		if err != nil {
			return bucketClient, fmt.Errorf("Error creating s3 client: %+v\n", err)
		}
		awsS3Client := s3.NewFromConfig(cfg)
		bucketClient = &S3Client{Client: awsS3Client}
	default:
		return bucketClient, fmt.Errorf("Unknown cloud provider: %s", c.Provider)
	}

	return bucketClient, nil
}

func (c AppConfig) NotifierFromConfig() (Notifier, error) {
	var notifier Notifier

	switch c.Notify.Service {
	case "sns":
		cfg, cfgErr := config.LoadDefaultConfig(context.TODO(),
			config.WithSharedConfigProfile(c.Notify.Profile),
			config.WithRegion(c.Notify.Region))

		if cfgErr != nil {
			return notifier, cfgErr
		}
		snsClient := sns.NewFromConfig(cfg)
		notifier = &SNSNotifier{Client: snsClient, Topic: c.Notify.ID}
	}

	return notifier, nil
}

func (c AppConfig) ConfigStringArray() []string {
	configStrArr := make([]string, 0)
	configStrArr = append(configStrArr, fmt.Sprintf("  - Region: %s", c.Provider.Region))
	configStrArr = append(configStrArr, fmt.Sprintf("  - IAMProfile: %s", c.Provider.Profile))
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
