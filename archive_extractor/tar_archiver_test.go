//go:build tests_group_all

package archive_extractor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTarUnexpectedEofArchiver(t *testing.T) {
	za := &TarArchiver{}
	funcParams := params()
	if err := za.ExtractArchive("./fixtures/test.deb", processingFunc, funcParams); err != nil {
		fmt.Print(err.Error() + "\n")
		assert.Equal(t, "archive/tar: invalid tar header", strings.Trim(err.Error(), ""))
	}
}

func TestTarArchiver(t *testing.T) {
	za := &TarArchiver{}
	funcParams := params()
	if err := za.ExtractArchive("./fixtures/test.tar.gz", processingFunc, funcParams); err != nil {
		fmt.Print(err.Error())
		t.Fatal(err)
	}
	ad := funcParams["archiveData"].(*ArchiveData)
	assert.Equal(t, ad.Name, "logRotator-1.0/log_rotator.go")
	assert.Equal(t, ad.ModTime, int64(1531307652))
	assert.Equal(t, ad.IsFolder, false)
	assert.Equal(t, ad.Size, int64(3685))
}

func TestTarArchiver_NoSymlinksResolvingOption(t *testing.T) {
	za := &TarArchiver{}
	funcParams := params()

	if err := za.ExtractArchiveWithOptions("./fixtures/test.tar.gz", processingFunc, funcParams, WithNoSymlinksResolving()); err != nil {
		fmt.Print(err.Error())
		t.Fatal(err)
	}
	ad := funcParams["archiveData"].(*ArchiveData)
	assert.Equal(t, "logRotator-1.0/log_rotator.go", ad.Name)
	assert.Equal(t, int64(1531307652), ad.ModTime)
	assert.False(t, ad.IsFolder)
	assert.Equal(t, int64(3685), ad.Size)
}

func TestTarArchiver_ResolvedSymlinksOption(t *testing.T) {
	za := &TarArchiver{}
	funcParams := params()

	if err := za.ExtractArchiveWithOptions("./fixtures/test.tar.gz", processingFunc, funcParams, WithResolvedSymlinks(SymLinksMap{})); err != nil {
		fmt.Print(err.Error())
		t.Fatal(err)
	}
	ad := funcParams["archiveData"].(*ArchiveData)
	assert.Equal(t, "logRotator-1.0/log_rotator.go", ad.Name)
	assert.Equal(t, int64(1531307652), ad.ModTime)
	assert.False(t, ad.IsFolder)
	assert.Equal(t, int64(3685), ad.Size)
}

func TestTarArchiver_Lzma(t *testing.T) {
	za := &TarArchiver{}
	funcParams := params()
	if err := za.ExtractArchive("./fixtures/junit.tar.lzma", processingFunc, funcParams); err != nil {
		fmt.Print(err.Error())
		t.Fatal(err)
	}
	ad := funcParams["archiveData"].(*ArchiveData)
	assert.Equal(t, "junit-4.12.jar", ad.Name)
	assert.Equal(t, int64(1534397548), ad.ModTime)
	assert.False(t, ad.IsFolder)
	assert.Equal(t, int64(314932), ad.Size)
}

func TestTarArchiverMaxRatio(t *testing.T) {
	za := &TarArchiver{
		MaxCompressRatio: 2,
	}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testsinglelarge.tar.gz", processingReadingFunc, funcParams)
	assert.True(t, IsErrCompressLimitReached(err))
}

func TestTarArchiverMaxRatioNotReached(t *testing.T) {
	za := &TarArchiver{
		MaxCompressRatio: 100,
	}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testsinglelarge.tar.gz", processingReadingFunc, funcParams)
	assert.NoError(t, err)
}

func TestTarArchiverMaxEntriesReached(t *testing.T) {
	za := &TarArchiver{
		MaxNumberOfEntries: 12,
	}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testmanylarge.tar.gz", processingReadingFunc, funcParams)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), ErrTooManyEntries.Error())
}

func TestTarArchiverMaxEntriesNotReached(t *testing.T) {
	za := &TarArchiver{
		MaxNumberOfEntries: 20,
	}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testmanylarge.tar.gz", processingReadingFunc, funcParams)
	assert.NoError(t, err)
}

func TestTarArchiverAggregationCauseRatioLimitError(t *testing.T) {
	za := &TarArchiver{
		MaxCompressRatio: 4,
	}
	funcParams := params()
	err := za.ExtractArchive("./fixtures/testmanylarge.tar.gz", processingReadingFunc, funcParams)
	assert.True(t, IsErrCompressLimitReached(err))
}

func TestTarArchiver_TarLz(t *testing.T) {
	za := &TarArchiver{}
	funcParams := params()
	if err := za.ExtractArchive("./fixtures/archive.tar.lz", processingFunc, funcParams); err != nil {
		fmt.Print(err.Error())
		t.Fatal(err)
	}
	ad := funcParams["archiveData"].(*ArchiveData)
	assert.Equal(t, ad.Name, "archive/commons-cli-1.2.jar")
	assert.Equal(t, ad.IsFolder, false)
	assert.Equal(t, ad.Size, int64(41123))
}

func TestExtractOptions(t *testing.T) {
	t.Parallel()

	noResolveOpts, err := applyExtractOptions([]ExtractOption{WithNoSymlinksResolving()})
	assert.NoError(t, err)
	assert.True(t, noResolveOpts.noSymlinksResolving)
	assert.False(t, noResolveOpts.withResolvedSymlinks)

	withLinksOpts, err := applyExtractOptions([]ExtractOption{WithResolvedSymlinks(SymLinksMap{"target": {"link"}})})
	assert.NoError(t, err)
	assert.True(t, withLinksOpts.withResolvedSymlinks)
	assert.Equal(t, SymLinksMap{"target": {"link"}}, withLinksOpts.symLinksMap)

	emptyMapOpts, err := applyExtractOptions([]ExtractOption{WithResolvedSymlinks(SymLinksMap{})})
	assert.NoError(t, err)
	assert.True(t, emptyMapOpts.withResolvedSymlinks)
	assert.Empty(t, emptyMapOpts.symLinksMap)

	noOptions, err := applyExtractOptions(nil)
	assert.NoError(t, err)
	assert.False(t, noOptions.noSymlinksResolving)
	assert.False(t, noOptions.withResolvedSymlinks)

	_, err = applyExtractOptions([]ExtractOption{WithResolvedSymlinks(nil)})
	assert.ErrorIs(t, err, ErrResolvedSymlinksMapRequired)

	_, err = applyExtractOptions([]ExtractOption{WithNoSymlinksResolving(), WithResolvedSymlinks(SymLinksMap{})})
	assert.ErrorIs(t, err, ErrConflictingSymlinksOptions)
}
