package main

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
