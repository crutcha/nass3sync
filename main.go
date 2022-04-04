package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	//"github.com/davecgh/go-spew/spew"
	"github.com/go-co-op/gocron"
	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	// TODO: put me into config or as env var!
	//log.SetLevel(log.DebugLevel)

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
		config.WithRegion(appConfig.AWSRegion))

	if err != nil {
		log.Fatal(fmt.Errorf("Error creating s3 client: %+v\n", err))
	}

	awsS3Client := s3.NewFromConfig(cfg)
	scheduler := gocron.NewScheduler(time.UTC)

	for _, sc := range appConfig.Sync {
		syncHandler := NewSyncHandler(awsS3Client, sc)
		scJob, scErr := scheduler.Every(sc.Interval).Minutes().Do(syncHandler.Sync)
		if scErr != nil {
			log.Fatal(scErr)
		}
		logString := fmt.Sprintf(
			"Scheduled sync for folder %s. Next run at: %s",
			sc.SourceFolder,
			scJob.ScheduledTime().String(),
		)
		log.Info(logString)
	}

	for _, bc := range appConfig.Backup {
		bcJob, bcErr := scheduler.Cron(bc.At).Do(tarAndUploadBackup, bc, awsS3Client)
		if bcErr != nil {
			log.Fatal(bcErr)
		}
		logString := fmt.Sprintf(
			"Scheduled backup for folder %s. Next run at: %s",
			bc.SourceFolder,
			bcJob.ScheduledTime().String(),
		)
		log.Info(logString)
	}

	scheduler.StartBlocking()
}
