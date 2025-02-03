package utils

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	FolderSuffix string = "/"
)

func PlaceHolderFolder(path string) bool {
	nameParts := strings.Split(path, "/")
	if len(nameParts) > 0 {
		return nameParts[len(nameParts)-1] == "-"
	} else {
		return false
	}
}

func IsFolder(path string) bool {
	return strings.HasSuffix(path, FolderSuffix)
}

func IsSymlink(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink == os.ModeSymlink
}

func GetLinkName(path string) string {
	info, err := os.Lstat(path)
	if err != nil {
		return ""
	}
	return info.Name()
}

// In Windows, filepath.Clean operation will replace all slashes '/'
// to backslashes '\\'
// This can mess-up with the code that makes path comparisons - in indexer-app on Windows
func CleanPathKeepingUnixSlash(path string) string {
	return filepath.ToSlash(filepath.Clean(path))
}

func JoinPathKeepingUnixSlash(elem ...string) string {
	return filepath.ToSlash(filepath.Join(elem...))
}
