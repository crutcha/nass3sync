package main

import "fmt"

type AppConfig struct {
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
}

type BackupConfig struct {
	SourceFolder      string `required:"true"`
	DestinationBucket string `required:"true"`
	At                string `required:"true"`
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
