package storage

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestBuffer(t *testing.T) {
	buffer := NewBuffer()

	// Test initial state
	if buffer.Size() != 0 {
		t.Errorf("Expected initial size 0, got %d", buffer.Size())
	}

	// Test writing data
	testData := []byte("Hello, World!")
	n, err := buffer.WriteBytes(testData)
	if err != nil {
		t.Errorf("Unexpected error writing to buffer: %v", err)
	}

	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
	}

	if buffer.Size() != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), buffer.Size())
	}

	// Test reading data
	reader := buffer.Reader()
	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("Unexpected error reading from buffer: %v", err)
	}

	if !bytes.Equal(testData, readData) {
		t.Errorf("Expected data %v, got %v", testData, readData)
	}

	// Test reset
	buffer.Reset()
	if buffer.Size() != 0 {
		t.Errorf("Expected size 0 after reset, got %d", buffer.Size())
	}
}

func TestBufferConcurrentAccess(t *testing.T) {
	buffer := NewBuffer()
	done := make(chan bool)

	// Test concurrent writes
	go func() {
		for i := 0; i < 100; i++ {
			buffer.WriteBytes([]byte("test"))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			buffer.WriteBytes([]byte("data"))
		}
		done <- true
	}()

	<-done
	<-done

	// Verify final size
	expectedSize := int64(800) // 100 * 4 bytes for "test" + 100 * 4 bytes for "data"
	if buffer.Size() != expectedSize {
		t.Errorf("Expected final size %d, got %d", expectedSize, buffer.Size())
	}
}

func TestBufferLargeData(t *testing.T) {
	buffer := NewBuffer()
	largeData := make([]byte, 1024*1024) // 1MB

	// Fill with test data
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Write large data
	n, err := buffer.WriteBytes(largeData)
	if err != nil {
		t.Errorf("Unexpected error writing large data: %v", err)
	}

	if n != len(largeData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(largeData), n)
	}

	if buffer.Size() != int64(len(largeData)) {
		t.Errorf("Expected size %d, got %d", len(largeData), buffer.Size())
	}

	// Read and verify
	reader := buffer.Reader()
	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("Unexpected error reading large data: %v", err)
	}

	if !bytes.Equal(largeData, readData) {
		t.Error("Large data read doesn't match written data")
	}
}

func TestStorageInterface(t *testing.T) {
	// Test that Buffer implements the Storage interface
	var _ Storage = &Buffer{}
}

func TestBufferMultipleWrites(t *testing.T) {
	buffer := NewBuffer()

	// Write multiple times
	writes := [][]byte{
		[]byte("Hello"),
		[]byte(", "),
		[]byte("World"),
		[]byte("!"),
	}

	totalSize := int64(0)
	for _, data := range writes {
		n, err := buffer.WriteBytes(data)
		if err != nil {
			t.Errorf("Unexpected error writing %v: %v", data, err)
		}
		totalSize += int64(n)
	}

	if buffer.Size() != totalSize {
		t.Errorf("Expected total size %d, got %d", totalSize, buffer.Size())
	}

	// Read all data
	reader := buffer.Reader()
	allData, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("Unexpected error reading all data: %v", err)
	}

	expectedData := []byte("Hello, World!")
	if !bytes.Equal(allData, expectedData) {
		t.Errorf("Expected data %v, got %v", expectedData, allData)
	}
}

func TestBufferStorageInterface(t *testing.T) {
	buffer := NewBuffer()
	ctx := context.Background()

	// Test Write method from Storage interface
	testData := "Hello, World!"
	reader := strings.NewReader(testData)
	err := buffer.Write(ctx, "test.txt", reader)
	if err != nil {
		t.Errorf("Unexpected error writing via Storage interface: %v", err)
	}

	// Test Read method from Storage interface
	readCloser, err := buffer.Read(ctx, "test.txt")
	if err != nil {
		t.Errorf("Unexpected error reading via Storage interface: %v", err)
	}
	defer readCloser.Close()

	readData, err := io.ReadAll(readCloser)
	if err != nil {
		t.Errorf("Unexpected error reading data: %v", err)
	}

	if string(readData) != testData {
		t.Errorf("Expected data '%s', got '%s'", testData, string(readData))
	}

	// Test List method from Storage interface
	files, err := buffer.List(ctx, "")
	if err != nil {
		t.Errorf("Unexpected error listing files: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected at least one file in list")
	}
}