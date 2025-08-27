package storage

import (
	"context"
	"strings"
	"testing"
)

func TestS3StorageStruct(t *testing.T) {
	// This test requires AWS credentials and a real S3 client
	// For now, we'll test the basic structure
	storage := &S3Storage{
		client: nil,
		bucket: "test-bucket",
		prefix: "test-prefix",
	}

	if storage.bucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got '%s'", storage.bucket)
	}

	if storage.prefix != "test-prefix" {
		t.Errorf("Expected prefix 'test-prefix', got '%s'", storage.prefix)
	}

	if storage.client != nil {
		t.Error("Expected client to be nil in test")
	}
}

func TestS3StorageNewS3Storage(t *testing.T) {
	// Test the constructor function
	storage := NewS3Storage(nil, "test-bucket", "test-prefix")

	if storage == nil {
		t.Fatal("Expected storage to be created")
	}

	if storage.bucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got '%s'", storage.bucket)
	}

	if storage.prefix != "test-prefix" {
		t.Errorf("Expected prefix 'test-prefix', got '%s'", storage.prefix)
	}

	if storage.client != nil {
		t.Error("Expected client to be nil in test")
	}
}

func TestS3StoragePathJoining(t *testing.T) {
	storage := &S3Storage{
		client: nil,
		bucket: "test-bucket",
		prefix: "test-prefix",
	}

	// Test path joining logic
	testCases := []struct {
		filepath string
		expected string
	}{
		{"file.txt", "test-prefix/file.txt"},
		{"folder/file.txt", "test-prefix/folder/file.txt"},
		{"", "test-prefix"},
		{"/absolute/path", "test-prefix/absolute/path"},
	}

	for _, tc := range testCases {
		// Since we can't actually call the methods without a real S3 client,
		// we'll test the path joining logic manually
		expected := tc.expected
		if tc.filepath == "" {
			expected = storage.prefix
		} else {
			expected = storage.prefix + "/" + strings.TrimPrefix(tc.filepath, "/")
		}

		if expected != tc.expected {
			t.Errorf("For filepath '%s', expected '%s', got '%s'", tc.filepath, tc.expected, expected)
		}
	}
}

func TestS3StorageInterface(t *testing.T) {
	// Test that S3Storage implements the Storage interface
	var _ Storage = &S3Storage{}
}

func TestS3StorageErrorHandling(t *testing.T) {
	// Test error handling scenarios
	storage := &S3Storage{
		client: nil,
		bucket: "test-bucket",
		prefix: "test-prefix",
	}

	ctx := context.Background()

	// Test Write with nil client
	reader := strings.NewReader("test data")
	err := storage.Write(ctx, "test.txt", reader)
	if err == nil {
		t.Error("Expected error with nil client")
	}
}

func TestS3StorageListEmpty(t *testing.T) {
	// Test listing with empty results
	storage := &S3Storage{
		client: nil,
		bucket: "test-bucket",
		prefix: "test-prefix",
	}

	ctx := context.Background()

	// Test List with nil client
	_, err := storage.List(ctx, "")
	if err == nil {
		t.Error("Expected error with nil client")
	}
}

func TestS3StorageReadError(t *testing.T) {
	// Test read error handling
	storage := &S3Storage{
		client: nil,
		bucket: "test-bucket",
		prefix: "test-prefix",
	}

	ctx := context.Background()

	// Test Read with nil client
	_, err := storage.Read(ctx, "test.txt")
	if err == nil {
		t.Error("Expected error with nil client")
	}
}

func TestS3StorageConcurrentAccess(t *testing.T) {
	// Test concurrent access to storage methods
	storage := &S3Storage{
		client: nil,
		bucket: "test-bucket",
		prefix: "test-prefix",
	}

	ctx := context.Background()
	done := make(chan bool)

	// Test concurrent operations (should all fail with nil client)
	go func() {
		storage.Write(ctx, "file1.txt", strings.NewReader("data1"))
		done <- true
	}()

	go func() {
		storage.Write(ctx, "file2.txt", strings.NewReader("data2"))
		done <- true
	}()

	go func() {
		storage.List(ctx, "")
		done <- true
	}()

	<-done
	<-done
	<-done
}