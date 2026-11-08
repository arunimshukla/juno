package db

import "context"

// Represents a read-only view of the database at a specific point in time.
// If you don't need to read at a specific time, use the database directly.
type Snapshot interface {
	KeyValueReader
	Close() error
}

// Produces a read-only snapshot of the database
type Snapshotter interface {
	NewSnapshot() Snapshot
}

// OpenSnapshot opens a consistent snapshot, allowing remote stores to report connection errors.
func OpenSnapshot(ctx context.Context, store Snapshotter) (Snapshot, error) {
	if opener, ok := store.(interface {
		OpenSnapshot(context.Context) (Snapshot, error)
	}); ok {
		return opener.OpenSnapshot(ctx)
	}
	return store.NewSnapshot(), nil
}
