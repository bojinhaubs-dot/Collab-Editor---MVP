package model

import "time"

const (
	RoleOwner  = "owner"
	RoleEditor = "editor"
	RoleViewer = "viewer"
	RoleNone   = "none"
)

// Document is the latest committed snapshot. Version starts at zero.
// Ownership is recorded here, not in document_members.
type Document struct {
	ID        string    `gorm:"column:id;type:varchar(64);primaryKey"`
	Title     string    `gorm:"column:title;type:varchar(255);not null"`
	Content   string    `gorm:"column:content;type:longtext;not null"`
	Version   int64     `gorm:"column:version;type:bigint;not null;default:0"`
	OwnerID   string    `gorm:"column:owner_id;type:varchar(64);not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime(3);not null"`
}

func (Document) TableName() string { return "documents" }

// DocumentMember records a non-owner's current role. A none role is retained
// so a permission change can revoke an existing member without deleting history.
type DocumentMember struct {
	DocumentID string `gorm:"column:document_id;type:varchar(64);primaryKey"`
	UserID     string `gorm:"column:user_id;type:varchar(64);primaryKey"`
	Role       string `gorm:"column:role;type:varchar(16);not null;check:member_role_valid,role IN ('editor','viewer','none')"`
}

func (DocumentMember) TableName() string { return "document_members" }

// Operation is an immutable successful edit. Unique indexes enforce a single
// operation per document version and per (document, user, request) identity.
type Operation struct {
	ID          uint64    `gorm:"column:id;type:bigint unsigned;primaryKey;autoIncrement"`
	DocumentID  string    `gorm:"column:document_id;type:varchar(64);not null;uniqueIndex:ux_operations_document_version,priority:1;uniqueIndex:ux_operations_request,priority:1"`
	UserID      string    `gorm:"column:user_id;type:varchar(64);not null;uniqueIndex:ux_operations_request,priority:2"`
	RequestID   string    `gorm:"column:request_id;type:varbinary(128);not null;uniqueIndex:ux_operations_request,priority:3"`
	BaseVersion int64     `gorm:"column:base_version;type:bigint;not null"`
	Version     int64     `gorm:"column:version;type:bigint;not null;uniqueIndex:ux_operations_document_version,priority:2"`
	Pos         int64     `gorm:"column:pos;type:bigint;not null"`
	DeleteCount int64     `gorm:"column:delete_count;type:bigint;not null"`
	Text        string    `gorm:"column:text;type:longtext;not null"`
	PayloadHash string    `gorm:"column:payload_hash;type:char(64);not null"`
	CreatedAt   time.Time `gorm:"column:created_at;type:datetime(3);not null"`
}

func (Operation) TableName() string { return "operations" }
