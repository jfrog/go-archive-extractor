package archive_extractor

import (
	"context"
	"github.com/jfrog/go-archive-extractor/archive_extractor/archiver_errors"
	"github.com/jfrog/go-archive-extractor/compression"
	"time"
)

type GzMetadataArchiver struct {
	MaxCompressRatio int64
}

func (ga GzMetadataArchiver) ExtractArchive(ctx context.Context, path string,
	processingFunc func(context.Context, *ArchiveHeader, map[string]interface{}) error, params map[string]interface{}) error {

	maxBytesLimit, err := maxBytesLimit(path, ga.MaxCompressRatio)
	if err != nil {
		return err
	}
	provider := LimitAggregatingReadCloserProvider{
		Limit: maxBytesLimit,
	}
	cReader, _, err := compression.NewReader(path)
	if compression.IsGetReaderError(err) {
		return archiver_errors.New(err)
	}
	if err != nil {
		return err
	}
	countingReadCloser := provider.CreateLimitAggregatingReadCloser(cReader)
	defer countingReadCloser.Close()
	archiveHeader := NewArchiveHeader(countingReadCloser, "metadata", time.Now().Unix(), 0)
	err = processingFunc(ctx, archiveHeader, params)
	if err != nil {
		return err
	}
	return nil
}
