package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func NewSNSNotifier(appConfig AppConfig) (Notifier, error) {
	var notifier Notifier

	cfg, cfgErr := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile(appConfig.Notify.Profile),
		config.WithRegion(appConfig.Notify.Region))

	if cfgErr != nil {
		return notifier, cfgErr
	}
	snsClient := &SNSClient{sns.NewFromConfig(cfg)}
	notifier = &SNSNotifier{Client: snsClient, Topic: appConfig.Notify.ID}

	return notifier, nil

}

type SNSClientIface interface {
	PublishMessage(msg *sns.PublishInput) error
}

type SNSClient struct {
	Client *sns.Client
}

func (s *SNSClient) PublishMessage(msg *sns.PublishInput) error {
	_, publishErr := s.Client.Publish(context.TODO(), msg)
	return publishErr
}

type SNSNotifier struct {
	Client SNSClientIface
	Topic  string
}

func (s *SNSNotifier) NotifySyncResults(syncConfig SyncConfig, resultMap *ResultMap) error {
	// we only want to notify if something actually happened
	if len(resultMap.Tombstone) == 0 && len(resultMap.Upload) == 0 {
		return nil
	}

	// TODO: this has a maximum message size of 256KB, need to account for that
	notificationBody := ""
	if len(resultMap.Upload) != 0 {
		notificationBody += "Uploads:\n"
		for key, keyErr := range resultMap.Upload {
			notificationBody += fmt.Sprintf("  - %s => %v\n", key, keyErr)
		}

	}

	if len(resultMap.Tombstone) != 0 {
		notificationBody += "\n\nTombstones:\n"
		for key, keyErr := range resultMap.Tombstone {
			notificationBody += fmt.Sprintf("  - %s => %v\n", key, keyErr)
		}
	}

	if len(resultMap.Delete) != 0 {
		notificationBody += "\n\nDeleted:\n"
		for key, keyErr := range resultMap.Delete {
			notificationBody += fmt.Sprintf("  - %s => %v\n", key, keyErr)
		}
	}

	snsPublishReq := &sns.PublishInput{
		Message:  aws.String(notificationBody),
		TopicArn: aws.String(s.Topic),
		Subject:  aws.String(fmt.Sprintf("Sync results: %s -> %s", syncConfig.SourceFolder, syncConfig.DestinationBucket)),
	}
	publishErr := s.Client.PublishMessage(snsPublishReq)

	return publishErr

}

func (s *SNSNotifier) NotifyBackupResults(backupConfig BackupConfig, backupFile *os.File, backupErr error) error {
	fileStat, _ := backupFile.Stat()

	subject := fmt.Sprintf("Backup result: %s", backupConfig.SourceFolder)
	notificationBody := fmt.Sprintf("Backup File Name: %s\n", fileStat.Name())
	notificationBody += fmt.Sprintf("Backup File Size: %d\n", fileStat.Size())
	notificationBody += fmt.Sprintf("Error: %v\n", backupErr)

	snsPublishReq := &sns.PublishInput{
		Message:  aws.String(notificationBody),
		TopicArn: aws.String(s.Topic),
		Subject:  aws.String(subject),
	}
	publishErr := s.Client.PublishMessage(snsPublishReq)

	return publishErr
}
