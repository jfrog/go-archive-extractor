package archive_extractor

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecompressor_ExtractArchive_CompressedFile(t *testing.T) {
	dc := &Decompressor{}
	funcParams := params()
	var testCases = []struct {
		FilePath         string
		ExpectedName     string
		ExpectedModTime  int64
		ExpectedIsFolder bool
		ExpectedSize     int64
	}{
		{
			FilePath:         "./fixtures/test.txt.xz",
			ExpectedName:     "test.txt",
			ExpectedModTime:  1661433804,
			ExpectedIsFolder: false,
			ExpectedSize:     64,
		},
		{
			FilePath:         "./fixtures/test.txt.bz2",
			ExpectedName:     "test.txt",
			ExpectedModTime:  1661837894,
			ExpectedIsFolder: false,
			ExpectedSize:     43,
		},
		{
			FilePath:         "./fixtures/test.txt.gz",
			ExpectedName:     "test.txt",
			ExpectedModTime:  1661837894,
			ExpectedIsFolder: false,
			ExpectedSize:     36,
		},
		{
			FilePath:         "./fixtures/test.txt.lzma",
			ExpectedName:     "test.txt",
			ExpectedModTime:  1661837894,
			ExpectedIsFolder: false,
			ExpectedSize:     30,
		},
		{
			FilePath:         "./fixtures/test.txt.Z",
			ExpectedName:     "test.txt",
			ExpectedModTime:  1661434675,
			ExpectedIsFolder: false,
			ExpectedSize:     11,
		},
	}
	for _, tc := range testCases {
		if err := dc.ExtractArchive(tc.FilePath, processingFunc, funcParams); err != nil {
			fmt.Print(err.Error())
			t.Fatal(err)
		}
		ad, ok := funcParams["archiveData"].(*ArchiveData)
		assert.True(t, ok)
		assert.Equal(t, ad.Name, tc.ExpectedName)
		assert.Equal(t, ad.ModTime, tc.ExpectedModTime)
		assert.Equal(t, ad.IsFolder, tc.ExpectedIsFolder)
		assert.Equal(t, ad.Size, tc.ExpectedSize)
	}
}

func TestDecompressor_ExtractArchive_NotCompressedFile(t *testing.T) {
	dc := &Decompressor{}
	funcParams := params()
	err := dc.ExtractArchive("./fixtures/test.txt", processingFunc, funcParams)
	assert.EqualError(t, err, "file ./fixtures/test.txt is not compressed or the compression method is not supported")
}

func TestDecompressorMaxRatio(t *testing.T) {
	dc := &Decompressor{
		MaxCompressRatio: 2,
	}
	funcParams := params()
	err := dc.ExtractArchive("./fixtures/testsinglelarge.txt.xz", processingReadingFunc, funcParams)
	assert.True(t, IsErrCompressLimitReached(err))
}

func TestDecompressorMaxRatioNotReached(t *testing.T) {
	dc := &Decompressor{
		MaxCompressRatio: 100,
	}
	funcParams := params()
	err := dc.ExtractArchive("./fixtures/testsinglelarge.txt.xz", processingReadingFunc, funcParams)
	assert.NoError(t, err)
}
