package main

import (
	"io/fs"
	"time"
)

type mockFileInfo struct {
	timestamp time.Time
	isDir     bool
}

func (f *mockFileInfo) Name() string       { return "mockfile" }
func (f *mockFileInfo) Size() int64        { return 1 }
func (f *mockFileInfo) Mode() fs.FileMode  { return fs.ModePerm }
func (f *mockFileInfo) ModTime() time.Time { return f.timestamp }
func (f *mockFileInfo) IsDir() bool        { return f.isDir }
func (f *mockFileInfo) Sys() any           { return nil }
