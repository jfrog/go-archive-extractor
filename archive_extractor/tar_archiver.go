package archive_extractor

import (
	"context"

	"github.com/jfrog/go-archive-extractor/archive_extractor/archiver_errors"
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
	return extractWithSymlinks(ctx, path, ta.MaxNumberOfEntries, provider, processingFunc, params)
}
