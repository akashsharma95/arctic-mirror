package storage

import (
	"bytes"
	"context"
	"io"
	"sync"
)

type Buffer struct {
	buf  *bytes.Buffer
	size int64
	mu   sync.Mutex
}

func NewBuffer() *Buffer {
	return &Buffer{
		buf: bytes.NewBuffer(nil),
	}
}

func (b *Buffer) WriteBytes(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n, err = b.buf.Write(p)
	b.size += int64(n)
	return
}

func (b *Buffer) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
	b.size = 0
}

func (b *Buffer) Reader() io.Reader {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.NewReader(b.buf.Bytes())
}

// Implement Storage interface methods
func (b *Buffer) Write(ctx context.Context, filepath string, data io.Reader) error {
	// For buffer storage, we read from the reader and write to our buffer
	b.mu.Lock()
	defer b.mu.Unlock()
	
	// Read all data from the reader
	allData, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	
	// Write to our buffer
	_, err = b.buf.Write(allData)
	if err != nil {
		return err
	}
	
	b.size += int64(len(allData))
	return nil
}

func (b *Buffer) Read(ctx context.Context, filepath string) (io.ReadCloser, error) {
	// For buffer storage, we return the current buffer content
	return io.NopCloser(b.Reader()), nil
}

func (b *Buffer) List(ctx context.Context, prefix string) ([]string, error) {
	// For buffer storage, we return the current buffer as a single "file"
	if b.Size() > 0 {
		return []string{"buffer_content"}, nil
	}
	return []string{}, nil
}
