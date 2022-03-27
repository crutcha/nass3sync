package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCSClient struct {
	Client *storage.Client
}

func (s *GCSClient) ListObjects(bucketName string) (map[string]ObjectInfo, error) {
	objectMap := make(map[string]ObjectInfo)
	objIter := s.Client.Bucket(bucketName).Objects(context.TODO(), nil)
	for {
		attrs, err := objIter.Next()
		if err == iterator.Done {
			break

		}
		if err != nil {
			return objectMap, fmt.Errorf("Bucket(%q).Objects: %v", bucketName, err)

		}
		objectMap[attrs.Name] = ObjectInfo{ModTime: attrs.Updated, Size: attrs.Size}
	}

	return objectMap, nil
}

func (s *GCSClient) UploadFile(bucketName, key string, file *os.File) error {
	object := s.Client.Bucket(bucketName).Object(key)
	objWriter := object.NewWriter(context.TODO())
	if _, uploadErr := io.Copy(objWriter, file); uploadErr != nil {
		return uploadErr
	}
	if closeErr := objWriter.Close(); closeErr != nil {
		return closeErr
	}

	return nil
}

func (s *GCSClient) CopyObject(sourceBucket, destinationBucket, key string) error {
	key = strings.TrimPrefix(key, "/")
	src := s.Client.Bucket(sourceBucket).Object(key)
	dst := s.Client.Bucket(destinationBucket).Object(key)

	if _, err := dst.CopierFrom(src).Run(context.TODO()); err != nil {
		return err
	}

	return nil
}

func (s *GCSClient) DeleteObject(bucket string, key string) error {
	key = strings.TrimPrefix(key, "/")
	object := s.Client.Bucket(bucket).Object(key)

	if err := object.Delete(context.TODO()); err != nil {
		return err
	}

	return nil
}
