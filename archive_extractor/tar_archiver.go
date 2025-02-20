package archive_extractor

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

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

	if err = ta.checkSpaceAvailability(ctx, path, maxBytesLimit); err != nil {
		return archiver_errors.New(err)
	}

	fsys, err := archives.FileSystem(ctx, path, nil)
	if err != nil {
		return archiver_errors.New(err)
	}

	symlinks := make(map[string][]string)

	if err = resolveSymlinks(fsys, symlinks); err != nil {
		return archiver_errors.NewOpenError(path, err)
	}

	provider := LimitAggregatingReadCloserProvider{
		Limit: maxBytesLimit,
	}

	if err = processArchive(fsys, symlinks, provider, processingFunc, params); err != nil {
		return archiver_errors.NewOpenError(path, err)
	}

	return nil
}

func (ta TarArchiver) checkSpaceAvailability(ctx context.Context, path string, maxBytesLimit int64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return archiver_errors.New(err)
	}
	if stat.Bavail >= uint64(maxBytesLimit) {
		return nil
	}

	format, _, err := archives.Identify(ctx, path, nil)
	if err != nil {
		return archiver_errors.New(err)
	}

	tarFile, err := os.Open(path)
	if err != nil {
		return archiver_errors.NewOpenError(path, err)
	}
	defer func() {
		_ = tarFile.Close()
	}()

	extractor, ok := format.(archives.Extractor)
	if !ok {
		return archiver_errors.New(archiver_errors.TarDecodeError)
	}

	var size int64
	err = extractor.Extract(ctx, tarFile, func(ctx context.Context, fileInfo archives.FileInfo) error {
		if ta.MaxNumberOfEntries != 0 && size >= int64(ta.MaxNumberOfEntries) {
			return ErrTooManyEntries
		}
		if !fileInfo.IsDir() && !utils.PlaceHolderFolder(fileInfo.Name()) {
			size += fileInfo.Size()
		}
		if size > maxBytesLimit {
			return ErrNotEnoughSpace
		}
		return nil
	})
	return err
}

func resolveSymlinks(fsys fs.FS, symlinks map[string][]string) error {
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

func processArchive(fsys fs.FS, symlinks map[string][]string, provider LimitAggregatingReadCloserProvider, processingFunc func(*ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {
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
