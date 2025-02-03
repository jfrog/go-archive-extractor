package archive_extractor

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jfrog/go-archive-extractor/archive_extractor/archiver_errors"
	"github.com/jfrog/go-archive-extractor/compression"
	"github.com/jfrog/go-archive-extractor/utils"
	"github.com/palantir/stacktrace"
)

type TarArchiver struct {
	MaxCompressRatio   int64
	MaxNumberOfEntries int
}

type TarFileInfo struct {
	Name      string
	Path      string
	Links     []string
	Size      int64
	Timestamp int64
}

type TarballFileIterator struct {
	path          string
	file          *os.File
	fileReader    io.ReadCloser
	tarballReader *tar.Reader
	symlinks      map[string][]string
}

func NewTarballFileIterator(archivePath string, maxCompressRatio int64) (TarballFileIterator, error) {
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return TarballFileIterator{}, stacktrace.Propagate(err, "Failed to open file '%s': %v")
	}

	maxBytesLimit, err := maxBytesLimit(archivePath, maxCompressRatio)
	if err != nil {
		return TarballFileIterator{}, err
	}
	provider := LimitAggregatingReadCloserProvider{
		Limit: maxBytesLimit,
	}
	cReader, _, err := compression.NewReader(archivePath)
	if compression.IsGetReaderError(err) {
		return TarballFileIterator{}, archiver_errors.New(err)
	}
	if err != nil {
		return TarballFileIterator{}, stacktrace.Propagate(err, "Failed to create reader for '%s'", archivePath)
	}
	limitingReader := provider.CreateLimitAggregatingReadCloser(cReader)
	rc := tar.NewReader(limitingReader)

	return TarballFileIterator{
		path:          archivePath,
		file:          archiveFile,
		fileReader:    limitingReader,
		tarballReader: rc,
	}, nil
}

func (iterator *TarballFileIterator) Close() error {
	var err1 error = nil
	if iterator.fileReader != nil {
		err1 = iterator.fileReader.Close()
	}

	err2 := iterator.file.Close()
	return errors.Join(err1, err2)
}

func (iterator *TarballFileIterator) ResolveSymlinks(ctx context.Context, maxCompressRatio int64) error {
	itr, err := NewTarballFileIterator(iterator.path, maxCompressRatio)
	if err != nil {
		return stacktrace.Propagate(err, "Failed to create file for symlink resolution '%s'", iterator.path)
	}

	iterator.symlinks = make(map[string][]string)

	for {
		hdr, err := itr.tarballReader.Next()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return stacktrace.Propagate(err, "Failed to iterate over files in '%s'", iterator.path)
		}

		if hdr.Typeflag == tar.TypeSymlink {
			path := cleanPath(hdr)
			realPath := realPath(hdr)

			paths, ok := iterator.symlinks[realPath]
			if !ok {
				paths = []string{}
			}

			iterator.symlinks[realPath] = append(paths, path)
		}
	}
}

func (iterator *TarballFileIterator) Reader() *tar.Reader {
	return iterator.tarballReader
}

func (iterator *TarballFileIterator) Next() (TarFileInfo, error) {
	for {
		hdr, err := iterator.tarballReader.Next()
		if err != nil {
			if err != io.EOF {
				err = stacktrace.Propagate(err, "Failed to iterate over files in '%s'", iterator.path)
			}
			return TarFileInfo{}, err

		}

		if hdr.FileInfo().IsDir() || utils.PlaceHolderFolder(hdr.FileInfo().Name()) {
			// File should be skipped and not processed
			continue
		}

		if hdr.Typeflag != tar.TypeReg {
			// we need only follow regular files here
			continue
		}

		path := cleanPath(hdr)
		links, ok := iterator.symlinks[path]
		if !ok {
			links = []string{}
		}

		return TarFileInfo{
			//Name:      hdr.Name,
			Path:      path,
			Links:     links,
			Size:      hdr.Size,
			Timestamp: hdr.FileInfo().ModTime().Unix(),
		}, nil
	}
}

func cleanPath(hdr *tar.Header) string {
	path := hdr.Name
	path = utils.CleanPathKeepingUnixSlash(path)
	return strings.TrimPrefix(path, "/")
}

func realPath(hdr *tar.Header) string {
	path := cleanPath(hdr)
	if hdr.Typeflag != tar.TypeSymlink {
		return path
	}

	currentDir, _ := filepath.Split(path)
	if filepath.IsAbs(hdr.Linkname) {
		return utils.CleanPathKeepingUnixSlash(hdr.Linkname)
	}

	return utils.JoinPathKeepingUnixSlash(currentDir, hdr.Linkname)
}

func (ta TarArchiver) ExtractArchive(ctx context.Context, archivePath string, processingFunc func(context.Context, *ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {
	iterator, err := NewTarballFileIterator(archivePath, ta.MaxCompressRatio)
	if err != nil {
		return stacktrace.Propagate(err, "Failed to create tar iterator for '%s'", archivePath)
	}
	defer func() {
		_ = iterator.Close()
	}()

	err = iterator.ResolveSymlinks(ctx, ta.MaxCompressRatio)
	if err != nil {
		return stacktrace.Propagate(err, "Failed resolving links for '%s'", archivePath)
	}

	entriesCount := 0
	for {
		if ta.MaxNumberOfEntries != 0 && entriesCount > ta.MaxNumberOfEntries {
			return ErrTooManyEntries
		}
		entriesCount++

		archiveEntry, err := iterator.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return stacktrace.Propagate(err, "Failed iterating files over '%s'", archivePath)
		}

		paths := []string{archiveEntry.Path}
		paths = append(paths, archiveEntry.Links...)

		for _, path := range paths {
			filename := filepath.Base(path)
			archiveHeader := NewArchiveHeader(iterator.Reader(), filename, archiveEntry.Timestamp, archiveEntry.Size)
			err = processingFunc(ctx, archiveHeader, params)
			if err != nil {
				continue
			}
		}
	}
	return nil
}
