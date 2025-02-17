package archive_extractor

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/jfrog/go-archive-extractor/archive_extractor/archiver_errors"
	"github.com/jfrog/go-archive-extractor/utils"
	"github.com/mholt/archives"
)

type TarArchiver struct {
	MaxCompressRatio   int64
	MaxNumberOfEntries int
}

func (ta TarArchiver) ExtractArchive(path string,
	processingFunc func(*ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {
	ctx := context.Background()

	fileSystem, err := archives.FileSystem(ctx, path, nil)
	if err != nil {
		return archiver_errors.NewOpenError(path, err)
	}

	provider, err := ta.getProvider(path)
	if err != nil {
		return archiver_errors.New(err)
	}

	symlinks := make(map[string][]string)

	err = resolveSymlinks(fileSystem, symlinks)
	if err != nil {
		return archiver_errors.NewProcessError(path, err)
	}

	err = processArchive(*provider, fileSystem, symlinks, processingFunc, params)
	if err != nil {
		return archiver_errors.NewProcessError(path, err)
	}

	return nil
}

func (ta TarArchiver) getProvider(path string) (*LimitAggregatingReadCloserProvider, error) {
	maxBytesLimit, err := maxBytesLimit(path, ta.MaxCompressRatio)
	if err != nil {
		return nil, err
	}
	provider := &LimitAggregatingReadCloserProvider{
		Limit: maxBytesLimit,
	}
	return provider, nil
}

func resolveSymlinks(fileSystem fs.FS, symlinks map[string][]string) error {
	return fs.WalkDir(fileSystem, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if isSymlink(d.Type()) {
			fileInfo, err := d.Info()
			if err != nil {
				return err
			}

			hdr, ok := fileInfo.(archives.FileInfo)
			if !ok {
				return nil
			}

			var realPath string
			cleanedPath := strings.TrimPrefix(utils.CleanPathKeepingUnixSlash(path), "/")
			if filepath.IsAbs(hdr.LinkTarget) {
				realPath = utils.CleanPathKeepingUnixSlash(hdr.LinkTarget)
			} else {
				currentDir, _ := filepath.Split(cleanedPath)
				realPath = utils.JoinPathKeepingUnixSlash(currentDir, hdr.LinkTarget)
			}

			paths, ok := symlinks[realPath]
			if !ok {
				paths = []string{}
			}
			symlinks[realPath] = append(paths, cleanedPath)
		}
		return nil
	})
}

func isSymlink(mode fs.FileMode) bool {
	return mode&fs.ModeSymlink != 0
}

func processArchive(provider LimitAggregatingReadCloserProvider, fileSystem fs.FS, symlinks map[string][]string,
	processingFunc func(*ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {
	return fs.WalkDir(fileSystem, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !utils.PlaceHolderFolder(d.Name()) {
			fileInfo, err := d.Info()
			if err != nil {
				return err
			}
			file, err := fileSystem.Open(path)
			if err != nil {
				return err
			}
			countingReadCloser := provider.CreateLimitAggregatingReadCloser(file)
			paths := []string{path}
			links, ok := symlinks[path]
			if ok {
				paths = append(paths, links...)
			}
			for _, p := range paths {
				filename := filepath.Base(p)
				archiveHeader := NewArchiveHeader(countingReadCloser, filename, fileInfo.ModTime().Unix(), fileInfo.Size())
				err = processingFunc(archiveHeader, params)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
