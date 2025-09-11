package compression

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archives"
	"github.com/ulikunitz/xz"
	"github.com/ulikunitz/xz/lzma"
)

const (
	bz2Ext   = ".bz2"
	tbz2Ext  = ".tbz2"
	gzExt    = ".gz"
	tgzExt   = ".tgz"
	lzwExt   = ".Z"
	inflExt  = ".infl"
	zlibExt  = ".xp3"
	xzExt    = ".xz"
	txzExt   = ".txz"
	lzmaExt  = ".lzma"
	tlzmaExt = ".tlzma"
	zstdExt  = ".zst"
	lzipExt  = ".lz"

	maxMagicBytes = 6 // 6 is the biggest used here (xz)
)

var (
	gzipMagic = []byte{0x1F, 0x8B}
	bz2Magic  = []byte{0x42, 0x5A, 0x68}
	xzMagic   = []byte{0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00}
	lzmaMagic = []byte{0x5D, 0x00, 0x00}
	zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}
	lzipMagic = []byte{0x4C, 0x5A, 0x49, 0x50} // "LZIP"
)

const defaultBufSize = 32 * 1024

type readerConfiguration struct {
	BufSize   int
	SkipBytes int64
}

type Option func(*readerConfiguration)

func WithSkipBytes(skipBytes int64) Option {
	return func(c *readerConfiguration) {
		c.SkipBytes = skipBytes
	}
}

func WithBufSize(size int) Option {
	return func(c *readerConfiguration) {
		c.BufSize = size
	}
}

func NewReader(filePath string, options ...Option) (io.ReadCloser, bool, error) {
	config := &readerConfiguration{BufSize: defaultBufSize, SkipBytes: 0}
	for _, option := range options {
		option(config)
	}
	return newReader(&fileArgs{path: filePath, skipBytes: config.SkipBytes}, config)
}

type fileArgs struct {
	path      string
	skipBytes int64
}

func (fa *fileArgs) open() (*os.File, error) {
	f, err := os.Open(fa.path)
	if err != nil {
		return nil, err
	}
	if fa.skipBytes > 0 {
		_, err := f.Seek(fa.skipBytes, io.SeekStart)
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

func newReader(fa *fileArgs, conf *readerConfiguration) (reader io.ReadCloser, isCompressed bool, err error) {
	isCompressed = true
	ext := filepath.Ext(fa.path)
	//these types has no defined magic bytes
	switch ext {
	case lzwExt:
		reader, err = initReader(fa, conf, lzwReader)
		return
	case inflExt:
		reader, err = initReader(fa, conf, flateReader)
		return
	case zlibExt:
		reader, err = initReader(fa, conf, zlibReader)
		return
	case lzipExt:
		reader, err = initReader(fa, conf, lzipReader)
		return
	}
	// if possible init by magic bytes
	if magic, magicErr := getMagicBytes(fa); magicErr == nil {
		switch {
		case bytes.HasPrefix(magic, bz2Magic):
			reader, err = initReader(fa, conf, bz2Reader)
			return
		case bytes.HasPrefix(magic, gzipMagic):
			reader, err = initReader(fa, conf, gzipReader)
			return
		case bytes.HasPrefix(magic, xzMagic):
			reader, err = initReader(fa, conf, xzReader)
			return
		case bytes.HasPrefix(magic, lzmaMagic):
			reader, err = initReader(fa, conf, lzmaReader)
			return
		case bytes.HasPrefix(magic, lzipMagic):
			reader, err = initReader(fa, conf, lzipReader)
			return
		case bytes.HasPrefix(magic, zstdMagic):
			reader, err = initReader(fa, conf, zstdReader)
			return
		}
	}
	// fallback to init by extension
	switch ext {
	case bz2Ext, tbz2Ext:
		reader, err = initReader(fa, conf, bz2Reader)
		return
	case gzExt, tgzExt:
		reader, err = initReader(fa, conf, gzipReader)
		return
	case xzExt, txzExt:
		reader, err = initReader(fa, conf, xzReader)
		return
	case lzmaExt, tlzmaExt:
		reader, err = initReader(fa, conf, lzmaReader)
		return
	case zstdExt:
		reader, err = initReader(fa, conf, zstdReader)
		return
	default:
		// no compression format found
		isCompressed = false
		reader, err = initReader(fa, conf, fileReader)
		return
	}
}

func getMagicBytes(fa *fileArgs) ([]byte, error) {
	f, err := fa.open()
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b := make([]byte, maxMagicBytes)
	if _, err = f.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// compression readers

type cReader struct {
	reader io.ReadCloser
	file   *os.File
}

func (cr *cReader) Read(p []byte) (int, error) {
	return cr.reader.Read(p)
}

func (cr *cReader) Close() error {
	if err := cr.file.Close(); err != nil {
		return err
	}
	if err := cr.reader.Close(); err != nil {
		return err
	}
	return nil
}

func initReader(fa *fileArgs, conf *readerConfiguration, getReader func(io.Reader) (io.ReadCloser, error)) (io.ReadCloser, error) {

	f, err := fa.open()
	if err != nil {
		return nil, err
	}
	r, err := getReader(bufio.NewReaderSize(f, conf.BufSize))
	if err != nil {
		f.Close()
		return nil, &ErrGetReader{err}
	}

	return &cReader{reader: r, file: f}, nil
}

type ErrGetReader struct {
	err error
}

func (e *ErrGetReader) Error() string {
	return e.err.Error()
}

func IsGetReaderError(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if _, ok := e.(*ErrGetReader); ok {
			return true
		}
	}
	return false
}

func bz2Reader(reader io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(bzip2.NewReader(reader)), nil
}

func flateReader(reader io.Reader) (io.ReadCloser, error) {
	return flate.NewReader(reader), nil
}

func gzipReader(reader io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(reader)
}

func lzwReader(reader io.Reader) (io.ReadCloser, error) {
	return lzw.NewReader(reader, lzw.LSB, 100), nil
}

func zlibReader(reader io.Reader) (io.ReadCloser, error) {
	return zlib.NewReader(reader)
}

func xzReader(reader io.Reader) (io.ReadCloser, error) {
	r, err := xz.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(r), nil
}

func lzmaReader(reader io.Reader) (io.ReadCloser, error) {
	r, err := lzma.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(r), nil
}

func lzipReader(reader io.Reader) (io.ReadCloser, error) {
	r, err := archives.Lzip{}.OpenReader(reader)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(r), nil
}

func zstdReader(reader io.Reader) (io.ReadCloser, error) {
	r, err := zstd.NewReader(reader, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(r), nil
}

func fileReader(reader io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(reader), nil
}
