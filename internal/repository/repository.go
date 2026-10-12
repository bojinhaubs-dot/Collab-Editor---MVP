package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"collab-mvp/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Repository owns the MySQL connection pool. Callers supply contexts that
// bound each database operation.
type Repository struct {
	db *gorm.DB
}

func Open(dsn string) (*Repository, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	return &Repository{db: db}, nil
}

func (r *Repository) AutoMigrate(ctx context.Context) (err error) {
	// Separate test packages and concurrently started servers may migrate the
	// same empty database at once. MySQL DDL commits implicitly, so serialize
	// AutoMigrate with a session-level advisory lock rather than a transaction.
	pool, err := r.db.DB()
	if err != nil {
		return fmt.Errorf("get mysql pool: %w", err)
	}
	conn, err := pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get migration connection: %w", err)
	}
	defer conn.Close()
	const lockName = "collab_mvp_schema_migration"
	var acquired int
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", lockName).Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if acquired != 1 {
		return fmt.Errorf("acquire migration lock: timed out")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released int
		releaseErr := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
		if err == nil && (releaseErr != nil || released != 1) {
			err = fmt.Errorf("release migration lock: result %d: %v", released, releaseErr)
		}
	}()
	db := r.db.WithContext(ctx).Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
	if err := db.AutoMigrate(&model.Document{}, &model.DocumentMember{}, &model.Operation{}); err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}
	return nil
}

// CreateDocument inserts a version-zero snapshot. The owner is the creator;
// there is no separate membership row for the owner.
func (r *Repository) CreateDocument(ctx context.Context, title, content, ownerID string) (model.Document, error) {
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return model.Document{}, fmt.Errorf("generate document id: %w", err)
	}
	doc := model.Document{
		ID:      "doc-" + hex.EncodeToString(idBytes),
		Title:   title,
		Content: content,
		Version: 0,
		OwnerID: ownerID,
	}
	if err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(&doc).Error
	}); err != nil {
		return model.Document{}, fmt.Errorf("create document: %w", err)
	}
	return doc, nil
}

// LoadDocument returns a consistent document snapshot and explicit member
// roles. The locking read waits for an uncertain edit or permission COMMIT
// that already holds the document row, so recovery cannot publish a state
// from before that transaction finishes.
func (r *Repository) LoadDocument(ctx context.Context, id string) (model.Document, map[string]string, error) {
	var doc model.Document
	var members []model.DocumentMember
	if err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&doc, "id = ?", id).Error; err != nil {
			return err
		}
		return tx.Where("document_id = ?", id).Find(&members).Error
	}); err != nil {
		return model.Document{}, nil, fmt.Errorf("load document %q: %w", id, err)
	}
	roles := make(map[string]string, len(members))
	for _, member := range members {
		roles[member.UserID] = member.Role
	}
	return doc, roles, nil
}
