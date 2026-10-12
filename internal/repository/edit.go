package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"collab-mvp/internal/model"
)

var (
	ErrVersionConflict = errors.New("document version conflict")
	// ErrCommitUnknown means the database did not confirm whether COMMIT took
	// effect. The caller must stop writes and reconcile by request ID and state.
	ErrCommitUnknown = errors.New("transaction commit result unknown")
)

// CommitEdit atomically advances the document snapshot and appends its log
// entry. A zero-row conditional update rejects a stale base version. No
// success is reported until the transaction's COMMIT is confirmed.
func (r *Repository) CommitEdit(ctx context.Context, documentID string, baseVersion int64, nextContent string, op model.Operation) error {
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("begin edit transaction: %w", tx.Error)
	}
	commitAttempted := false
	defer func() {
		if !commitAttempted {
			_ = tx.Rollback().Error
		}
	}()

	result := tx.Model(&model.Document{}).
		Where("id = ? AND version = ?", documentID, baseVersion).
		Updates(map[string]any{
			"content":    nextContent,
			"version":    baseVersion + 1,
			"updated_at": time.Now().UTC(),
		})
	if result.Error != nil {
		return fmt.Errorf("update document for edit: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("document %q base version %d: %w", documentID, baseVersion, ErrVersionConflict)
	}

	op.DocumentID = documentID
	op.BaseVersion = baseVersion
	op.Version = baseVersion + 1
	if err := tx.Create(&op).Error; err != nil {
		return fmt.Errorf("insert operation: %w", err)
	}

	commitAttempted = true
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("commit edit: %w: %w", ErrCommitUnknown, err)
	}
	return nil
}
