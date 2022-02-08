package archive_extractor

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestZipUnexpectedEofArchiver(t *testing.T) {
	za := &ZipArchiver{}
	funcParams := params()
	if err := za.ExtractArchive("./fixtures/test.deb", processingFunc, funcParams); err != nil {
		fmt.Print(err.Error() + "\n")
		assert.Equal(t, "No zip file found", strings.Trim(err.Error(), ""))
	}
}

func TestZipArchiver(t *testing.T) {
	za := &ZipArchiver{}
	funcParams := params()
	if err := za.ExtractArchive("./fixtures/test.zip", processingFunc, funcParams); err != nil {
		fmt.Print(err.Error())
		t.Fatal(err)
	}
	ad := funcParams["archiveData"].(*ArchiveData)
	assert.Equal(t, ad.Name, "test.txt")
	assert.Equal(t, ad.ModTime, int64(1534147868))
	assert.Equal(t, ad.IsFolder, false)
	assert.Equal(t, ad.Size, int64(0))
}

func TestZipArchiverReadAll(t *testing.T) {
	za := &ZipArchiver{}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/test.zip", processingReadingFunc, funcParams)
	assert.NoError(t, err)
	assert.Zero(t, funcParams["read"])
}

func TestZipArchiverReadAllWithEntry(t *testing.T) {
	za := &ZipArchiver{MaxCompressRatio: 1}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testwithcontent.zip", processingReadingFunc, funcParams)
	assert.NoError(t, err)
	assert.Equal(t, int64(13), funcParams["read"])
}

func TestZipArchiverReadAllWithEntryMaxNumberOfEntriesOk(t *testing.T) {
	za := &ZipArchiver{MaxCompressRatio: 1, MaxNumberOfEntries: 100}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testwithmanyfiles.zip", processingReadingFunc, funcParams)
	assert.NoError(t, err)
}

func TestZipArchiverReadAllWithEntryMaxNumberOfEntriesTooHigh(t *testing.T) {
	za := &ZipArchiver{MaxCompressRatio: 1, MaxNumberOfEntries: 99}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testwithmanyfiles.zip", processingReadingFunc, funcParams)
	assert.EqualError(t, err, ErrTooManyEntries.Error())
}

func TestZipArchiverRatioAndMaxEntriesNotSet(t *testing.T) {
	za := &ZipArchiver{}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testwithcontent.zip", processingReadingFunc, funcParams)
	assert.NoError(t, err)
	assert.Equal(t, int64(13), funcParams["read"])
}

func TestZipArchiverRatioNotSet(t *testing.T) {
	za := &ZipArchiver{MaxNumberOfEntries: 1000}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testwithcontent.zip", processingReadingFunc, funcParams)
	assert.NoError(t, err)
	assert.Equal(t, int64(13), funcParams["read"])
}

func TestZipArchiverAggregationCauseError(t *testing.T) {
	za := &ZipArchiver{
		MaxCompressRatio: 1,
	}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testmanyfileswithcontent.zip", processingReadingFunc, funcParams)
	assert.True(t, IsErrCompressLimitReached(err))
}

func TestZipArchiverSingleFileRatioCauseError(t *testing.T) {
	za := &ZipArchiver{
		MaxCompressRatio: 1,
	}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testwithsinglelargefile.zip", processingReadingFunc, funcParams)
	assert.True(t, IsErrCompressLimitReached(err))
}

func TestZipArchiver_AppendedZip(t *testing.T) {
	appendedZipPath := "./fixtures/appendedZip"
	za := &ZipArchiver{}
	funcParams := params()
	_, err := zip.OpenReader(appendedZipPath)
	assert.True(t, errors.Is(err, zip.ErrFormat))
	err = za.ExtractArchive(appendedZipPath, processingFunc, funcParams)
	assert.NoError(t, err)
}
