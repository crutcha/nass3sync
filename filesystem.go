package main

import (
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
