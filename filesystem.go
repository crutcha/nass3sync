package main

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
)

type walkFunc func(string) (map[string]os.FileInfo, error)

func walkDirectory(dirPath string) (map[string]os.FileInfo, error) {
	fileMap := make(map[string]os.FileInfo)
	walkErr := filepath.Walk(dirPath, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			fileMap[path] = f

		}
		return nil
	})

	return fileMap, walkErr
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
