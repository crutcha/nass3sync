package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type AppConfig struct {
	Provider    string `required:"true"`
	AWSRegion   string `required:"true"`
	IAMProfile  string
	Concurrency int `default:"1"`
	Sync        []SyncConfig
	Backup      []BackupConfig
	SNSTopic    string
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

	switch c.Provider {
	case "aws":
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithSharedConfigProfile(c.IAMProfile),
			config.WithRegion(c.AWSRegion))
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

func (c AppConfig) ConfigStringArray() []string {
	configStrArr := make([]string, 0)
	configStrArr = append(configStrArr, fmt.Sprintf("  - AWSRegion: %s", c.AWSRegion))
	configStrArr = append(configStrArr, fmt.Sprintf("  - IAMProfile: %s", c.IAMProfile))
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
