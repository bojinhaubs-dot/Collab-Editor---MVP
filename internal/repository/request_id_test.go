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

// Idempotency keys are byte identities. The database's default utf8mb4
// collation must not collapse different IDs before the Room can compare them.
func TestRequestIDIsCaseAndSpaceSensitive(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run MySQL request ID test")
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
	doc, err := r.CreateDocument(ctx, "request-id-test", "abc", "owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		r.db.WithContext(cleanupCtx).Where("document_id = ?", doc.ID).Delete(&model.Operation{})
		r.db.WithContext(cleanupCtx).Where("id = ?", doc.ID).Delete(&model.Document{})
	})
	for i, id := range []string{"case-A", "case-a", "case-a "} {
		op := model.Operation{UserID: "owner", RequestID: id, Pos: int64(i + 1), Text: "X", PayloadHash: strings.Repeat("a", 64)}
		if err := r.CommitEdit(ctx, doc.ID, int64(i), "abc"+strings.Repeat("X", i+1), op); err != nil {
			t.Fatalf("commit distinct request ID %q: %v", id, err)
		}
	}
	if _, err := r.GetOperation(ctx, doc.ID, "owner", "CASE-A"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("case-folded lookup = %v, want not found", err)
	}
	for i, id := range []string{"case-A", "case-a", "case-a "} {
		op, err := r.GetOperation(ctx, doc.ID, "owner", id)
		if err != nil || op.Version != int64(i+1) {
			t.Fatalf("lookup %q = %+v, %v", id, op, err)
		}
	}
	if count, err := r.CountOperations(ctx, doc.ID); err != nil || count != 3 {
		t.Fatalf("operation count = %d, %v; want 3", count, err)
	}
}
