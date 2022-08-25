package archive_extractor

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecompressor_ExtractArchive_CompressedFile(t *testing.T) {
	dc := &Decompressor{}
	funcParams := params()
	if err := dc.ExtractArchive("./fixtures/test.txt.xz", processingFunc, funcParams); err != nil {
		fmt.Print(err.Error())
		t.Fatal(err)
	}
	ad := funcParams["archiveData"].(*ArchiveData)
	assert.Equal(t, ad.Name, "test.txt")
	assert.Equal(t, ad.ModTime, int64(1661433804))
	assert.Equal(t, ad.IsFolder, false)
	assert.Equal(t, ad.Size, int64(64))
}

func TestDecompressor_ExtractArchive_NotCompressedFile(t *testing.T) {
	dc := &Decompressor{}
	funcParams := params()
	err := dc.ExtractArchive("./fixtures/test.txt", processingFunc, funcParams)
	assert.Error(t, err, "file is not compressed or the compression method is not supported")
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
