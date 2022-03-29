package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
)

func main() {
	// TODO: put me into config or as env var!
	log.SetLevel(log.DebugLevel)

	configFilePath := flag.String("configfile", "", "Configuration File Path")
	flag.Parse()

	if *configFilePath == "" {
		panic("Required flag -configfile not set but required")
	}

	var appConfig AppConfig
	configErr := configor.Load(&appConfig, *configFilePath)
	if configErr != nil {
		log.Fatal(configErr)
	}

	log.Info(fmt.Sprintf("config: %#v\n", appConfig))
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile(appConfig.IAMProfile),
		config.WithRegion(appConfig.BucketRegion))

	if err != nil {
		log.Fatal(fmt.Errorf("Error creating s3 client: %+v\n", err))
	}

	awsS3Client := s3.NewFromConfig(cfg)
	syncHandler := NewSyncHandler(awsS3Client, appConfig)
	fmt.Printf("%+v\n", syncHandler)
	log.Info(fmt.Sprintf("%+v\n", syncHandler))
	syncErr := syncHandler.Sync()
	if syncErr != nil {
		log.Fatal(fmt.Errorf("Sync handler error: %s\n", syncErr))
	}
}
