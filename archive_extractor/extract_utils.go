package archive_extractor

import (
	"context"
	"errors"
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

// SymLinksMap maps a symlink target path to the symlink paths that point to it.
type SymLinksMap map[string][]string

// symlinksMode selects how symlinks are handled during tar extraction.
type symlinksMode int

const (
	// resolveSymlinks scans the archive and resolves symlinks into a map (default).
	resolveSymlinks symlinksMode = iota
	// noSymlinksResolving skips symlink resolution and processes entries without symlink aliases.
	noSymlinksResolving
	// preResolvedSymlinks uses a caller-provided symlinks map.
	preResolvedSymlinks
)

type extractOptions struct {
	mode        symlinksMode
	symLinksMap SymLinksMap
}

// defaultExtractOptions returns the extract options used when no options are provided.
func defaultExtractOptions() extractOptions {
	return extractOptions{
		mode: resolveSymlinks,
	}
}

// ExtractOption configures tar extraction with symlinks.
type ExtractOption func(*extractOptions)

// WithNoSymlinksResolving skips symlink resolution and processes entries without symlink aliases.
func WithNoSymlinksResolving() ExtractOption {
	return func(o *extractOptions) {
		o.mode = noSymlinksResolving
	}
}

// WithResolvedSymlinks skips symlink resolution and uses a pre-resolved symlinks map.
// The map must be non-nil (an empty map is valid).
func WithResolvedSymlinks(m SymLinksMap) ExtractOption {
	return func(o *extractOptions) {
		o.mode = preResolvedSymlinks
		o.symLinksMap = m
	}
}

func applyExtractOptions(options ...ExtractOption) (extractOptions, error) {
	opts := defaultExtractOptions()
	for _, option := range options {
		option(&opts)
	}
	// Fall back to resolving symlinks from the archive when pre-resolved mode is
	// selected without a symlinks map.
	if opts.mode == preResolvedSymlinks && opts.symLinksMap == nil {
		opts.mode = resolveSymlinks
	}
	return opts, nil
}

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

// symlinksResolver returns the symlinks map to use for extraction.
type symlinksResolver func() (SymLinksMap, error)

// symlinksResolvers maps each symlinks mode to the resolver producing its symlinks map.
// Resolvers are lazy: only the one selected by opts.mode is invoked.
func (opts extractOptions) symlinksResolvers(ctx context.Context, ex archives.Extractor, path string, maxEntries int) map[symlinksMode]symlinksResolver {
	return map[symlinksMode]symlinksResolver{
		noSymlinksResolving: func() (SymLinksMap, error) {
			return SymLinksMap{}, nil
		},
		preResolvedSymlinks: func() (SymLinksMap, error) {
			return opts.symLinksMap, nil
		},
		resolveSymlinks: func() (SymLinksMap, error) {
			return resolveSymlinksFromArchive(ctx, ex, path, maxEntries)
		},
	}
}

// resolveSymlinksFromArchive scans the archive at path and builds its symlinks map.
func resolveSymlinksFromArchive(ctx context.Context, ex archives.Extractor, path string, maxEntries int) (SymLinksMap, error) {
	arcSymLinkReader, _, err := compression.NewReader(path)
	if compression.IsGetReaderError(err) {
		return nil, archiver_errors.New(err)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		arcSymLinkReader.Close()
	}()

	symlinks := make(SymLinksMap)
	if err = ResolveSymlinks(ctx, ex, arcSymLinkReader, maxEntries, symlinks); err != nil {
		return nil, err
	}
	return symlinks, nil
}

func extractWithSymlinks(ctx context.Context, path string, MaxNumberOfEntries int, provider LimitAggregatingReadCloserProvider, processingFunc processingArchiveFunc, params map[string]any, options ...ExtractOption) error {
	opts, err := applyExtractOptions(options...)
	if err != nil {
		return err
	}
	tarExtractor := archives.Tar{}

	resolve, ok := opts.symlinksResolvers(ctx, tarExtractor, path, MaxNumberOfEntries)[opts.mode]
	if !ok {
		return errors.New("unknown symlinks mode")
	}
	symlinks, err := resolve()
	if err != nil {
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

func ResolveSymlinks(ctx context.Context,
	ex archives.Extractor,
	arcReader io.Reader,
	MaxNumberOfEntries int,
	symlinks SymLinksMap) error {

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
	symlinks SymLinksMap,
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
