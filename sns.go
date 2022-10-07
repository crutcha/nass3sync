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

type NotificationContext struct {
	Action string
	Key    string
	Error  error
}

func (s *SNSNotifier) NotifySyncResults(syncConfig SyncConfig, resultMap *ResultMap) error {
	errors := make([]NotificationContext, 0)

	for key, err := range resultMap.Upload {
		if err != nil {
			errors = append(errors, NotificationContext{
				Action: "Upload",
				Key:    key,
				Error:  err,
			})
		}
	}

	for key, err := range resultMap.Delete {
		if err != nil {
			errors = append(errors, NotificationContext{
				Action: "Delete",
				Key:    key,
				Error:  err,
			})
		}
	}

	for key, err := range resultMap.Tombstone {
		if err != nil {
			errors = append(errors, NotificationContext{
				Action: "Tombstone",
				Key:    key,
				Error:  err,
			})
		}
	}

	// if no errors we dont need to send any notification
	if len(errors) == 0 {
		return nil
	}

	// TODO: this has a maximum message size of 256KB, need to account for that
	notificationBody := ""
	for _, ctx := range errors {
		notificationBody += fmt.Sprintf(
			"Action: %s\nKey: %s\nError: %s\n\n\n ",
			ctx.Action,
			ctx.Key,
			ctx.Error,
		)
	}

	snsPublishReq := &sns.PublishInput{
		Message:  aws.String(notificationBody),
		TopicArn: aws.String(s.Topic),
		Subject:  aws.String(fmt.Sprintf("Sync Errors: %s -> %s", syncConfig.SourceFolder, syncConfig.DestinationBucket)),
	}
	publishErr := s.Client.PublishMessage(snsPublishReq)

	return publishErr

}

func (s *SNSNotifier) NotifyBackupResults(backupConfig BackupConfig, backupFile *os.File, backupErr error) error {
	fileStat, _ := backupFile.Stat()

	var statusString string
	if backupErr == nil {
		statusString = "succeeded"
	} else {
		statusString = "failed"
	}

	subject := fmt.Sprintf("Backup %s: %s", statusString, backupConfig.SourceFolder)
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
