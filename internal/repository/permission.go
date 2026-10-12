package repository

import (
	"context"
	"fmt"

	"collab-mvp/internal/model"

	"gorm.io/gorm/clause"
)

// GetOperation finds a successful request by its per-user idempotency key.
// A missing operation retains gorm.ErrRecordNotFound for errors.Is callers.
func (r *Repository) GetOperation(ctx context.Context, documentID, userID, requestID string) (model.Operation, error) {
	var op model.Operation
	if err := r.db.WithContext(ctx).
		Where("document_id = ? AND user_id = ? AND request_id = ?", documentID, userID, requestID).
		First(&op).Error; err != nil {
		return model.Operation{}, fmt.Errorf("get operation %q for document %q and user %q: %w", requestID, documentID, userID, err)
	}
	return op, nil
}

// ChangePermission records the current non-owner role. A none row stays in
// the table, preserving the fact that access was explicitly revoked.
func (r *Repository) ChangePermission(ctx context.Context, documentID, targetUser, role string) error {
	switch role {
	case model.RoleEditor, model.RoleViewer, model.RoleNone:
	default:
		return fmt.Errorf("invalid document role %q", role)
	}
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("begin permission transaction: %w", tx.Error)
	}
	commitAttempted := false
	defer func() {
		if !commitAttempted {
			_ = tx.Rollback().Error
		}
	}()

	var doc model.Document
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "owner_id").First(&doc, "id = ?", documentID).Error; err != nil {
		return fmt.Errorf("load permission owner for document %q: %w", documentID, err)
	}
	if targetUser == doc.OwnerID {
		return fmt.Errorf("cannot change document owner's role")
	}
	member := model.DocumentMember{DocumentID: documentID, UserID: targetUser, Role: role}
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "document_id"}, {Name: "user_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"role"}),
	}).Create(&member).Error; err != nil {
		return fmt.Errorf("change permission for user %q in document %q: %w", targetUser, documentID, err)
	}
	commitAttempted = true
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("commit permission: %w: %w", ErrCommitUnknown, err)
	}
	return nil
}

func (r *Repository) CountOperations(ctx context.Context, documentID string) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&model.Operation{}).
		Where("document_id = ?", documentID).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("count operations for document %q: %w", documentID, err)
	}
	return count, nil
}
