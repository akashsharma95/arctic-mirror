package replication

import (
    "fmt"
    "os"
    "path/filepath"
    "strconv"
    "sync"

    "github.com/jackc/pglogrepl"
)

// LSNCheckpoint manages persistence of the last processed WAL LSN
type LSNCheckpoint struct {
    filePath string
    mu       sync.Mutex
}

func NewLSNCheckpoint(filePath string) *LSNCheckpoint {
    return &LSNCheckpoint{filePath: filePath}
}

// Load returns the saved LSN, or 0 if none exists
func (c *LSNCheckpoint) Load() (pglogrepl.LSN, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    data, err := os.ReadFile(c.filePath)
    if err != nil {
        if os.IsNotExist(err) {
            return pglogrepl.LSN(0), nil
        }
        return pglogrepl.LSN(0), fmt.Errorf("reading LSN checkpoint: %w", err)
    }

    // stored as decimal uint64
    u, err := strconv.ParseUint(string(bytesTrimSpace(data)), 10, 64)
    if err != nil {
        return pglogrepl.LSN(0), fmt.Errorf("parsing LSN checkpoint: %w", err)
    }
    return pglogrepl.LSN(u), nil
}

// Save persists the given LSN atomically
func (c *LSNCheckpoint) Save(lsn pglogrepl.LSN) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    dir := filepath.Dir(c.filePath)
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return fmt.Errorf("creating checkpoint dir: %w", err)
    }

    tmp := c.filePath + ".tmp"
    content := []byte(strconv.FormatUint(uint64(lsn), 10))
    if err := os.WriteFile(tmp, content, 0o644); err != nil {
        return fmt.Errorf("writing temp checkpoint: %w", err)
    }
    if err := os.Rename(tmp, c.filePath); err != nil {
        return fmt.Errorf("renaming checkpoint: %w", err)
    }
    return nil
}

func bytesTrimSpace(b []byte) []byte {
    // lightweight trim to avoid importing strings
    start := 0
    for start < len(b) && (b[start] == ' ' || b[start] == '\n' || b[start] == '\r' || b[start] == '\t') {
        start++
    }
    end := len(b)
    for end > start && (b[end-1] == ' ' || b[end-1] == '\n' || b[end-1] == '\r' || b[end-1] == '\t') {
        end--
    }
    return b[start:end]
}

