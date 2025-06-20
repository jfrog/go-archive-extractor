package archive_extractor

import (
	"context"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/mholt/archives"

	"github.com/jfrog/go-archive-extractor/archive_extractor/archiver_errors"
	"github.com/jfrog/go-archive-extractor/compression"
	"github.com/jfrog/go-archive-extractor/utils"
)

type processingArchiveFunc func(*ArchiveHeader, map[string]interface{}) error

func extract(ctx context.Context, ex archives.Extractor, arcReader io.Reader, MaxNumberOfEntries int, provider LimitAggregatingReadCloserProvider, processingFunc processingArchiveFunc, params map[string]any) error {
	entriesCount := 0
	var multiErrors *archiver_errors.MultiError
	err := ex.Extract(ctx, arcReader, func(ctx context.Context, fileInfo archives.FileInfo) error {
		if MaxNumberOfEntries != 0 && entriesCount >= MaxNumberOfEntries {
			return ErrTooManyEntries
		}
		entriesCount++
		file, err := fileInfo.Open()
		defer func() {
			if file != nil {
				_ = file.Close()
			}
		}()
		if err != nil {
			multiErrors = archiver_errors.Append(multiErrors, archiver_errors.NewArchiverExtractorError(fileInfo.NameInArchive, err))
		} else if !fileInfo.IsDir() && !utils.PlaceHolderFolder(fileInfo.Name()) {
			countingReadCloser := provider.CreateLimitAggregatingReadCloser(file)
			archiveHeader := NewArchiveHeader(countingReadCloser, fileInfo.NameInArchive, fileInfo.ModTime().Unix(), fileInfo.Size())
			processingError := processingFunc(archiveHeader, params)
			if processingError != nil {
				return processingError
			}
		}
		return nil
	})
	//multi error can be skipped or not skipped by caller, therefore we distinguish between err and multiErrors
	if err == nil && multiErrors != nil {
		return multiErrors
	}
	return err
}

func extractWithSymlinks(ctx context.Context, path string, MaxNumberOfEntries int, provider LimitAggregatingReadCloserProvider, processingFunc processingArchiveFunc, params map[string]any) error {
	arcSymLincReader, _, err := compression.NewReader(path)
	if compression.IsGetReaderError(err) {
		return archiver_errors.New(err)
	}
	if err != nil {
		return err
	}
	defer func() {
		arcSymLincReader.Close()
	}()

	tarExtractor := archives.Tar{}

	symlinks := make(map[string][]string)
	if err = resolveSymlinks(ctx, tarExtractor, arcSymLincReader, MaxNumberOfEntries, symlinks); err != nil {
		return err
	}
	arcReader, _, err := compression.NewReader(path)
	if compression.IsGetReaderError(err) {
		return archiver_errors.New(err)
	}
	if err != nil {
		return err
	}
	defer func() {
		arcReader.Close()
	}()

	return processArchiveAndSymlinks(ctx, tarExtractor, arcReader, MaxNumberOfEntries, symlinks, provider, processingFunc, params)
}

func resolveSymlinks(ctx context.Context,
	ex archives.Extractor,
	arcReader io.Reader,
	MaxNumberOfEntries int,
	symlinks map[string][]string) error {

	entriesCount := 0
	return ex.Extract(ctx, arcReader, func(ctx context.Context, fileInfo archives.FileInfo) error {
		if MaxNumberOfEntries != 0 && entriesCount >= MaxNumberOfEntries {
			return ErrTooManyEntries
		}
		entriesCount++
		if fileInfo.Mode().Type()&fs.ModeSymlink != 0 {
			cleanedPath := strings.TrimPrefix(utils.CleanPathKeepingUnixSlash(fileInfo.NameInArchive), "/")

			var realPath string
			if filepath.IsAbs(fileInfo.LinkTarget) {
				realPath = filepath.ToSlash(filepath.Clean(cleanedPath))
			} else {
				currentDir, _ := filepath.Split(cleanedPath)
				realPath = utils.JoinPathKeepingUnixSlash(currentDir, fileInfo.LinkTarget)
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

func processArchiveAndSymlinks(ctx context.Context,
	ex archives.Extractor,
	arcReader io.Reader,
	MaxNumberOfEntries int,
	symlinks map[string][]string,
	provider LimitAggregatingReadCloserProvider,
	processingFunc processingArchiveFunc,
	params map[string]any) error {

	entriesCount := 0
	var multiErrors *archiver_errors.MultiError
	err := ex.Extract(ctx, arcReader, func(ctx context.Context, fileInfo archives.FileInfo) error {
		if MaxNumberOfEntries != 0 && entriesCount >= MaxNumberOfEntries {
			return ErrTooManyEntries
		}
		entriesCount++
		file, err := fileInfo.Open()
		defer func() {
			if file != nil {
				_ = file.Close()
			}
		}()
		cleanedPath := strings.TrimPrefix(utils.CleanPathKeepingUnixSlash(fileInfo.NameInArchive), "/")
		if err != nil {
			multiErrors = archiver_errors.Append(multiErrors, archiver_errors.NewArchiverExtractorError(cleanedPath, err))
		} else if !fileInfo.IsDir() &&
			!utils.PlaceHolderFolder(fileInfo.Name()) &&
			// we skip symlinks here because we need to process their targets
			fileInfo.Mode().Type()&fs.ModeSymlink == 0 {
			paths := []string{cleanedPath}
			linkPaths, ok := symlinks[cleanedPath]
			if ok {
				paths = append(paths, linkPaths...)
			}
			for _, path := range paths {
				countingReadCloser := provider.CreateLimitAggregatingReadCloser(file)
				archiveHeader := NewArchiveHeader(countingReadCloser, path, fileInfo.ModTime().Unix(), fileInfo.Size())
				processingError := processingFunc(archiveHeader, params)
				if processingError != nil {
					return processingError
				}
			}
		}
		return nil
	})

	//multi error can be skipped or not skipped by caller, therefore we distinguish between err and multiErrors
	if err == nil && multiErrors != nil {
		return multiErrors
	}
	return err
}
