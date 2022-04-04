package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

func tarAndUploadBackup(backupConfig BackupConfig, awsClient *s3.Client) {
	// TODO: move file walk into util that returns list of paths
	filesToCompress := make([]string, 0)
	walkErr := filepath.Walk(backupConfig.SourceFolder, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			filesToCompress = append(filesToCompress, path)
		}
		return nil
	})
	if walkErr != nil {
		log.Error(fmt.Sprintf("Backup directory walk failed: %s", walkErr))
	}

	now := time.Now()
	backupTimestamp := now.Format(time.RFC3339)
	backupPrefix := fmt.Sprintf("%s_%s_*.tar.gz", filepath.Base(backupConfig.SourceFolder), backupTimestamp)
	tarFile, _ := ioutil.TempFile(os.TempDir(), backupPrefix)
	//defer os.Remove(tarFile.Name())

	log.Info(fmt.Sprintf("Creating backup tarball: %s", tarFile.Name()))
	//defer os.Remove(tarFile)
	createArchive(filesToCompress, tarFile)

	// this is janky but the way this is written, this file descripter would be closed already
	// so....we need to open the file again
	uploadFile, uploadFileOpenErr := os.Open(tarFile.Name())
	if uploadFileOpenErr != nil {
		log.Warn("Error uploading backup: ", uploadFileOpenErr)
		return
	}
	defer uploadFile.Close()
	uploader := manager.NewUploader(awsClient)
	fileKey := filepath.Base(tarFile.Name())
	_, putErr := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(backupConfig.DestinationBucket),
		Key:    aws.String(fileKey),
		Body:   uploadFile,
	})
	if putErr != nil {
		log.Warn("Backup upload error: ", putErr)
	} else {
		log.Info("Upload succeded for ", fileKey)
	}
}

func createArchive(files []string, buf io.Writer) error {
	gw := gzip.NewWriter(buf)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Iterate over files and add them to the tar archive
	for _, file := range files {
		err := addToArchive(tw, file)
		if err != nil {
			return err
		}
	}

	return nil
}

func addToArchive(tw *tar.Writer, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return err
	}

	header.Name = filename

	err = tw.WriteHeader(header)
	if err != nil {
		return err
	}

	_, err = io.Copy(tw, file)
	if err != nil {
		return err
	}

	return nil
}
