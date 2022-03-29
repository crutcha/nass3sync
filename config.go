package main

type AppConfig struct {
	SourceFolder      string `required:"true"`
	DestinationBucket string `required:"true"`
	BucketRegion      string `required:"true"`
	TombstoneBucket   string
	IAMProfile        string
}
