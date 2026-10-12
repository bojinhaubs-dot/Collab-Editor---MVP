package repository

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"collab-mvp/internal/model"
)

func TestCommitEditAtomicity(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run MySQL transaction test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AutoMigrate(ctx); err != nil {
		t.Fatal(err)
	}
	doc, err := r.CreateDocument(ctx, "transaction-test", "abc", "owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		r.db.WithContext(cleanupCtx).Where("document_id = ?", doc.ID).Delete(&model.Operation{})
		r.db.WithContext(cleanupCtx).Where("id = ?", doc.ID).Delete(&model.Document{})
	})
	op := model.Operation{
		UserID: "editor", RequestID: "edit-1", Pos: 1, Text: "X",
		PayloadHash: strings.Repeat("a", 64),
	}
	if err := r.CommitEdit(ctx, doc.ID, 0, "aXbc", op); err != nil {
		t.Fatalf("first edit: %v", err)
	}
	if err := r.CommitEdit(ctx, doc.ID, 0, "abYc", model.Operation{
		UserID: "editor", RequestID: "edit-2", Pos: 2, Text: "Y",
		PayloadHash: strings.Repeat("b", 64),
	}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale edit error = %v, want ErrVersionConflict", err)
	}
	// The conditional update succeeds, but the duplicate request ID causes
	// operation insertion to fail. Its snapshot change must roll back.
	if err := r.CommitEdit(ctx, doc.ID, 1, "aXXbc", op); err == nil {
		t.Fatal("duplicate operation unexpectedly succeeded")
	}
	got, _, err := r.LoadDocument(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "aXbc" || got.Version != 1 {
		t.Fatalf("snapshot = %q/v%d, want aXbc/v1", got.Content, got.Version)
	}
	var count int64
	if err := r.db.WithContext(ctx).Model(&model.Operation{}).Where("document_id = ?", doc.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("operation count = %d, want 1", count)
	}
}
