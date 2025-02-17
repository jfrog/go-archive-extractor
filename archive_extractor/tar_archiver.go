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

func (ta TarArchiver) ExtractArchive(path string, processingFunc func(*ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {
	ctx := context.Background()
	maxBytesLimit, err := maxBytesLimit(path, ta.MaxCompressRatio)
	if err != nil {
		return archiver_errors.New(err)
	}
	provider := LimitAggregatingReadCloserProvider{
		Limit: maxBytesLimit,
	}
	format, _, err := archives.Identify(ctx, path, nil)
	if err != nil {
		return archiver_errors.New(err)
	}
	extractor, ok := format.(archives.Extractor)
	if !ok {
		return archiver_errors.New(archiver_errors.TarDecodeError)
	}
	return extractWithSymlinks(ctx, extractor, path, ta.MaxNumberOfEntries, provider, processingFunc, params)
}

func (ta TarArchiver) ExtractArchive2(path string, processingFunc func(*ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {

	ctx := context.Background()
	maxBytesLimit, err := maxBytesLimit(path, ta.MaxCompressRatio)
	if err != nil {
		return archiver_errors.New(err)
	}

	fsys, err := archives.FileSystem(ctx, path, nil)
	if err != nil {
		return archiver_errors.New(err)
	}

	symlinks := make(map[string][]string)

	if err = resolveSymlinks2(fsys, symlinks); err != nil {
		return archiver_errors.NewOpenError(path, err)
	}

	provider := LimitAggregatingReadCloserProvider{
		Limit: maxBytesLimit,
	}

	if err = processArchive2(fsys, symlinks, provider, processingFunc, params); err != nil {
		return archiver_errors.NewOpenError(path, err)
	}

	return nil
}

func resolveSymlinks2(fsys fs.FS, symlinks map[string][]string) error {
	return fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			fileInfo, err := d.Info()
			if err != nil {
				return err
			}
			hdr, ok := fileInfo.(archives.FileInfo)
			if !ok {
				return nil
			}
			cleanedPath := strings.TrimPrefix(utils.CleanPathKeepingUnixSlash(path), "/")

			var realPath string
			if filepath.IsAbs(hdr.LinkTarget) {
				realPath = filepath.ToSlash(filepath.Clean(cleanedPath))
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

func processArchive2(fsys fs.FS, symlinks map[string][]string, provider LimitAggregatingReadCloserProvider, processingFunc func(*ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {
	return fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !utils.PlaceHolderFolder(d.Name()) {
			if d.Type()&fs.ModeSymlink != 0 {
				return nil
			}
			fileInfo, err := d.Info()
			if err != nil {
				return err
			}
			file, err := fsys.Open(path)
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
