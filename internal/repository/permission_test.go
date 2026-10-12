package repository

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"collab-mvp/internal/model"

	"gorm.io/gorm"
)

func TestPermissionAndOperationQueries(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run MySQL repository test")
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
	doc, err := r.CreateDocument(ctx, "permission-test", "abc", "owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		r.db.WithContext(cleanupCtx).Where("document_id = ?", doc.ID).Delete(&model.Operation{})
		r.db.WithContext(cleanupCtx).Where("document_id = ?", doc.ID).Delete(&model.DocumentMember{})
		r.db.WithContext(cleanupCtx).Where("id = ?", doc.ID).Delete(&model.Document{})
	})
	if err := r.ChangePermission(ctx, doc.ID, "alice", model.RoleEditor); err != nil {
		t.Fatal(err)
	}
	if err := r.ChangePermission(ctx, doc.ID, "alice", model.RoleViewer); err != nil {
		t.Fatal(err)
	}
	if err := r.ChangePermission(ctx, doc.ID, "alice", model.RoleNone); err != nil {
		t.Fatal(err)
	}
	loaded, members, err := r.LoadDocument(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.OwnerID != "owner" || members["alice"] != model.RoleNone || len(members) != 1 {
		t.Fatalf("document/roles = %+v/%+v, want owner and alice:none", loaded, members)
	}
	if err := r.ChangePermission(ctx, doc.ID, "owner", model.RoleViewer); err == nil {
		t.Fatal("owner role change unexpectedly succeeded")
	}
	if _, err := r.GetOperation(ctx, doc.ID, "alice", "missing"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("missing operation error = %v, want gorm.ErrRecordNotFound", err)
	}
	if count, err := r.CountOperations(ctx, doc.ID); err != nil || count != 0 {
		t.Fatalf("initial operation count = %d, err=%v", count, err)
	}
	op := model.Operation{UserID: "owner", RequestID: "test-1", Pos: 1, Text: "X", PayloadHash: strings.Repeat("a", 64)}
	if err := r.CommitEdit(ctx, doc.ID, 0, "aXbc", op); err != nil {
		t.Fatal(err)
	}
	got, err := r.GetOperation(ctx, doc.ID, "owner", "test-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.BaseVersion != 0 || got.Pos != 1 || got.Text != "X" {
		t.Fatalf("operation = %+v, want committed v1 edit", got)
	}
	if count, err := r.CountOperations(ctx, doc.ID); err != nil || count != 1 {
		t.Fatalf("final operation count = %d, err=%v", count, err)
	}
}
