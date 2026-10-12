package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"collab-mvp/internal/model"
	"collab-mvp/internal/protocol"
	"collab-mvp/internal/repository"
	"gorm.io/gorm"
)

// fakeStore keeps committed state separate from the Room's in-memory state.
// Blocking CommitEdit lets tests put subsequent commands in a known queue order.
type fakeStore struct {
	mu                sync.Mutex
	doc               model.Document
	members           map[string]string
	operations        map[string]model.Operation
	blockCommit       chan struct{}
	commitEntered     chan struct{}
	enteredOnce       sync.Once
	failCommit        error
	failPermission    error
	unknownCommit     bool
	unknownPermission bool
	failReload        atomic.Bool
	loadOverride      *model.Document
}

func newFakeStore(content string) *fakeStore {
	return &fakeStore{
		doc:        model.Document{ID: "doc-1", OwnerID: "owner", Content: content},
		members:    map[string]string{"bob": model.RoleEditor, "alice": model.RoleEditor, "viewer": model.RoleViewer},
		operations: make(map[string]model.Operation),
	}
}

func (s *fakeStore) LoadDocument(_ context.Context, documentID string) (model.Document, map[string]string, error) {
	if s.failReload.Load() {
		return model.Document{}, nil, errors.New("database reload unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if documentID != s.doc.ID {
		return model.Document{}, nil, gorm.ErrRecordNotFound
	}
	members := make(map[string]string, len(s.members))
	for user, role := range s.members {
		members[user] = role
	}
	if s.loadOverride != nil {
		return *s.loadOverride, members, nil
	}
	return s.doc, members, nil
}

func (s *fakeStore) CommitEdit(ctx context.Context, documentID string, baseVersion int64, nextContent string, op model.Operation) error {
	if s.blockCommit != nil && baseVersion == 0 {
		s.enteredOnce.Do(func() { close(s.commitEntered) })
		select {
		case <-s.blockCommit:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCommit != nil {
		return s.failCommit
	}
	if documentID != s.doc.ID || baseVersion != s.doc.Version {
		return repository.ErrVersionConflict
	}
	s.doc.Content = nextContent
	s.doc.Version++
	s.operations[opKey(documentID, op.UserID, op.RequestID)] = op
	if s.unknownCommit {
		return repository.ErrCommitUnknown
	}
	return nil
}

func (s *fakeStore) GetOperation(_ context.Context, documentID, userID, requestID string) (model.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[opKey(documentID, userID, requestID)]
	if !ok {
		return model.Operation{}, gorm.ErrRecordNotFound
	}
	return op, nil
}

func (s *fakeStore) ChangePermission(_ context.Context, documentID, targetUser, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failPermission != nil {
		return s.failPermission
	}
	if documentID != s.doc.ID {
		return gorm.ErrRecordNotFound
	}
	s.members[targetUser] = role
	if s.unknownPermission {
		return repository.ErrCommitUnknown
	}
	return nil
}

func (s *fakeStore) operationCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.operations)
}

func opKey(documentID, userID, requestID string) string {
	return documentID + "\x00" + userID + "\x00" + requestID
}

type fakeSubscriber struct {
	user     string
	messages chan any
	allow    atomic.Bool
	closed   chan struct{}
	once     sync.Once
}

func newSubscriber(user string) *fakeSubscriber {
	s := &fakeSubscriber{user: user, messages: make(chan any, 16), closed: make(chan struct{})}
	s.allow.Store(true)
	return s
}

func (s *fakeSubscriber) UserID() string { return s.user }

func (s *fakeSubscriber) Closed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *fakeSubscriber) Send(value any) bool {
	if !s.allow.Load() {
		return false
	}
	select {
	case s.messages <- value:
		return true
	default:
		return false
	}
}

func (s *fakeSubscriber) Close() { s.once.Do(func() { close(s.closed) }) }

func setupRoom(t *testing.T, store *fakeStore, users ...string) (*Room, map[string]*fakeSubscriber) {
	t.Helper()
	manager := NewManager(store)
	t.Cleanup(manager.Close)
	r, err := manager.Get(context.Background(), store.doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	subs := make(map[string]*fakeSubscriber, len(users))
	for _, user := range users {
		sub := newSubscriber(user)
		if err := r.Join(context.Background(), sub); err != nil {
			t.Fatalf("join %s: %v", user, err)
		}
		message := receive(t, sub)
		if messageType(t, message) != "snapshot" {
			t.Fatalf("join %s returned %v, want snapshot", user, message)
		}
		subs[user] = sub
	}
	return r, subs
}

func receive(t *testing.T, sub *fakeSubscriber) any {
	t.Helper()
	select {
	case msg := <-sub.messages:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s message", sub.user)
		return nil
	}
}

func messageType(t *testing.T, message any) string {
	t.Helper()
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Type
}

func messageFields(t *testing.T, message any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func drainMessages(sub *fakeSubscriber) []any {
	var messages []any
	for {
		select {
		case msg := <-sub.messages:
			messages = append(messages, msg)
		default:
			return messages
		}
	}
}

func expectType(t *testing.T, sub *fakeSubscriber, want string) any {
	t.Helper()
	msg := receive(t, sub)
	if got := messageType(t, msg); got != want {
		t.Fatalf("%s received %s, want %s: %#v", sub.user, got, want, msg)
	}
	return msg
}

func expectErrorCode(t *testing.T, sub *fakeSubscriber, want string) {
	t.Helper()
	msg := expectType(t, sub, "error")
	got, ok := msg.(map[string]any)["code"]
	if !ok || got != want {
		t.Fatalf("%s error code = %v, want %s", sub.user, got, want)
	}
}

func expectNoMessage(t *testing.T, sub *fakeSubscriber) {
	t.Helper()
	select {
	case msg := <-sub.messages:
		t.Fatalf("%s received unexpected message: %#v", sub.user, msg)
	case <-time.After(100 * time.Millisecond):
	}
}

func editMessage(t *testing.T, requestID string, baseVersion int64, op protocol.Splice) []byte {
	t.Helper()
	raw, err := json.Marshal(struct {
		Type        string          `json:"type"`
		RequestID   string          `json:"request_id"`
		BaseVersion int64           `json:"base_version"`
		Op          protocol.Splice `json:"op"`
	}{"edit", requestID, baseVersion, op})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func sendEdit(t *testing.T, r *Room, sub *fakeSubscriber, requestID string, baseVersion int64, op protocol.Splice) {
	t.Helper()
	if err := r.Handle(context.Background(), sub, editMessage(t, requestID, baseVersion, op)); err != nil {
		t.Fatal(err)
	}
}

func waitQueueLength(t *testing.T, r *Room, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(r.commands) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued commands = %d, want %d", len(r.commands), want)
}

func TestEditAndRevocationUseSameQueue(t *testing.T) {
	for _, tc := range []struct {
		name            string
		queueEditBefore bool
		wantVersion     int64
		wantContent     string
	}{
		{"edit before revocation", true, 2, "aXYbc"},
		{"revocation before edit", false, 1, "aXbc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore("abc")
			store.blockCommit = make(chan struct{})
			store.commitEntered = make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(store.blockCommit) }) }
			defer release()
			r, subs := setupRoom(t, store, "owner", "bob")
			bob := subs["bob"]
			sendEdit(t, r, bob, "first", 0, protocol.Splice{Pos: 1, Text: "X"})
			select {
			case <-store.commitEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("first edit did not reach CommitEdit")
			}
			permissionResult := make(chan error, 1)
			changePermission := func() {
				role, err := r.ChangePermission(context.Background(), "owner", "bob", model.RoleViewer)
				if err == nil && role != model.RoleViewer {
					err = fmt.Errorf("effective role = %q, want viewer", role)
				}
				permissionResult <- err
			}
			if tc.queueEditBefore {
				sendEdit(t, r, bob, "second", 1, protocol.Splice{Pos: 2, Text: "Y"})
				waitQueueLength(t, r, 1)
				go changePermission()
			} else {
				go changePermission()
				waitQueueLength(t, r, 1)
				sendEdit(t, r, bob, "second", 1, protocol.Splice{Pos: 2, Text: "Y"})
			}
			waitQueueLength(t, r, 2)
			release()
			if err := <-permissionResult; err != nil {
				t.Fatalf("change permission: %v", err)
			}
			expectType(t, bob, "applied")
			if tc.queueEditBefore {
				expectType(t, bob, "applied")
				expectType(t, bob, "permission_changed")
				expectType(t, subs["owner"], "applied")
				expectType(t, subs["owner"], "applied")
			} else {
				expectType(t, bob, "permission_changed")
				expectErrorCode(t, bob, "PERMISSION_DENIED")
				expectType(t, subs["owner"], "applied")
			}
			snap, err := r.Snapshot(context.Background(), "owner")
			if err != nil || snap.Content != tc.wantContent || snap.Version != tc.wantVersion || store.operationCount() != int(tc.wantVersion) {
				t.Fatalf("queue order changed committed state: snapshot=%+v, logs=%d, err=%v", snap, store.operationCount(), err)
			}
			// A fresh edit over the old connection after the successful HTTP-like
			// permission command must be denied even with the current version.
			sendEdit(t, r, bob, "after-revocation", tc.wantVersion, protocol.Splice{Pos: 0, Text: "Z"})
			expectErrorCode(t, bob, "PERMISSION_DENIED")
			expectNoMessage(t, subs["owner"])
		})
	}
}

func TestFailedEditTransactionDoesNotChangeStateOrBroadcast(t *testing.T) {
	store := newFakeStore("abc")
	store.failCommit = errors.New("transaction rolled back")
	r, subs := setupRoom(t, store, "owner", "alice")
	sendEdit(t, r, subs["alice"], "failed", 0, protocol.Splice{Pos: 1, Text: "X"})
	expectErrorCode(t, subs["alice"], "INTERNAL_ERROR")
	expectNoMessage(t, subs["owner"])
	snap, err := r.Snapshot(context.Background(), "alice")
	if err != nil || snap.Content != "abc" || snap.Version != 0 || store.operationCount() != 0 {
		t.Fatalf("failed transaction changed state: snapshot=%+v, logs=%d, err=%v", snap, store.operationCount(), err)
	}
}

func TestFailedPermissionTransactionDoesNotChangeRoleOrNotify(t *testing.T) {
	store := newFakeStore("abc")
	store.failPermission = errors.New("permission transaction rolled back")
	r, subs := setupRoom(t, store, "owner", "bob")
	if role, err := r.ChangePermission(context.Background(), "owner", "bob", model.RoleViewer); !errors.Is(err, ErrInternal) || role != "" {
		t.Fatalf("failed permission change = (%q, %v), want ErrInternal", role, err)
	}
	expectNoMessage(t, subs["bob"])
	snap, err := r.Snapshot(context.Background(), "bob")
	if err != nil || snap.Role != model.RoleEditor {
		t.Fatalf("failed transaction changed role: %+v, %v", snap, err)
	}
	sendEdit(t, r, subs["bob"], "still-editor", 0, protocol.Splice{Pos: 1, Text: "X"})
	expectType(t, subs["bob"], "applied")
	expectType(t, subs["owner"], "applied")
}

func TestSpliceUsesUnicodeCodePoints(t *testing.T) {
	store := newFakeStore("a中😀b")
	r, subs := setupRoom(t, store, "alice")
	sendEdit(t, r, subs["alice"], "replace-emoji", 0, protocol.Splice{Pos: 2, DeleteCount: 1, Text: "界"})
	expectType(t, subs["alice"], "applied")
	snap, err := r.Snapshot(context.Background(), "alice")
	if err != nil || snap.Content != "a中界b" || snap.Version != 1 {
		t.Fatalf("code point replacement: snapshot=%+v, err=%v", snap, err)
	}
	sendEdit(t, r, subs["alice"], "byte-offset", 1, protocol.Splice{Pos: 8, Text: "x"})
	expectErrorCode(t, subs["alice"], "INVALID_OPERATION")
	if store.operationCount() != 1 {
		t.Fatalf("invalid byte offset wrote operation log: %d", store.operationCount())
	}
}

func TestSlowSubscriberDoesNotBlockOtherConnections(t *testing.T) {
	store := newFakeStore("abc")
	r, subs := setupRoom(t, store, "alice", "viewer")
	subs["viewer"].allow.Store(false) // Its bounded send queue is now full.
	sendEdit(t, r, subs["alice"], "one", 0, protocol.Splice{Pos: 1, Text: "X"})
	expectType(t, subs["alice"], "applied")
	select {
	case <-subs["viewer"].closed:
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscriber was not closed")
	}
	sendEdit(t, r, subs["alice"], "two", 1, protocol.Splice{Pos: 2, Text: "Y"})
	expectType(t, subs["alice"], "applied")
	snap, err := r.Snapshot(context.Background(), "alice")
	if err != nil || snap.Content != "aXYbc" || snap.Version != 2 {
		t.Fatalf("healthy connection was blocked: snapshot=%+v, err=%v", snap, err)
	}
}

func TestConcurrentEditsWithSameBaseVersionRejectOne(t *testing.T) {
	store := newFakeStore("abc")
	r, subs := setupRoom(t, store, "owner", "alice", "bob")
	aliceEdit := editMessage(t, "alice-1", 0, protocol.Splice{Pos: 1, Text: "X"})
	bobEdit := editMessage(t, "bob-1", 0, protocol.Splice{Pos: 2, Text: "Y"})
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- r.Handle(context.Background(), subs["alice"], aliceEdit)
	}()
	go func() {
		<-start
		results <- r.Handle(context.Background(), subs["bob"], bobEdit)
	}()
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("enqueue concurrent edit: %v", err)
		}
	}
	// Snapshot is a queue barrier: both previously enqueued edits have finished.
	snap, err := r.Snapshot(context.Background(), "owner")
	if err != nil || snap.Version != 1 || (snap.Content != "aXbc" && snap.Content != "abYc") || store.operationCount() != 1 {
		t.Fatalf("same-base edits did not produce one commit: snapshot=%+v, logs=%d, err=%v", snap, store.operationCount(), err)
	}
	ownerMessages := drainMessages(subs["owner"])
	if len(ownerMessages) != 1 || messageType(t, ownerMessages[0]) != "applied" {
		t.Fatalf("owner messages = %#v, want one applied", ownerMessages)
	}
	var applied, conflicts int
	for _, user := range []string{"alice", "bob"} {
		for _, msg := range drainMessages(subs[user]) {
			fields := messageFields(t, msg)
			switch fields["type"] {
			case "applied":
				applied++
			case "error":
				if fields["code"] != "VERSION_CONFLICT" || fields["current_version"] != float64(1) {
					t.Fatalf("%s got wrong conflict: %#v", user, fields)
				}
				conflicts++
			default:
				t.Fatalf("%s got unexpected message: %#v", user, fields)
			}
		}
	}
	if applied != 2 || conflicts != 1 {
		t.Fatalf("client message counts: applied=%d conflicts=%d, want 2 broadcasts and 1 rejection", applied, conflicts)
	}
}

func TestUnknownCommitPausesWritesUntilReload(t *testing.T) {
	store := newFakeStore("abc")
	store.unknownCommit = true // The database committed, but its reply was lost.
	r, subs := setupRoom(t, store, "owner", "alice", "bob")
	store.failReload.Store(true)
	sendEdit(t, r, subs["alice"], "uncertain", 0, protocol.Splice{Pos: 1, Text: "X"})
	expectErrorCode(t, subs["alice"], "INTERNAL_ERROR")
	expectNoMessage(t, subs["owner"])
	if _, err := r.Snapshot(context.Background(), "owner"); !errors.Is(err, ErrBusy) {
		t.Fatalf("snapshot during uncertain commit = %v, want ErrBusy", err)
	}
	sendEdit(t, r, subs["bob"], "while-paused", 0, protocol.Splice{Pos: 2, Text: "Y"})
	expectErrorCode(t, subs["bob"], "SERVER_BUSY")
	if store.operationCount() != 1 {
		t.Fatalf("write proceeded while Room paused: logs=%d", store.operationCount())
	}
	store.failReload.Store(false)
	snap, err := r.Snapshot(context.Background(), "owner")
	if err != nil || snap.Content != "aXbc" || snap.Version != 1 {
		t.Fatalf("reload did not recover committed snapshot: %+v, %v", snap, err)
	}
	for _, user := range []string{"owner", "alice", "bob"} {
		expectType(t, subs[user], "applied")
	}
	// The same request now confirms the successful commit without another write.
	sendEdit(t, r, subs["alice"], "uncertain", 0, protocol.Splice{Pos: 1, Text: "X"})
	ack := messageFields(t, expectType(t, subs["alice"], "ack"))
	if ack["duplicate"] != true || ack["version"] != float64(1) || store.operationCount() != 1 {
		t.Fatalf("retry did not confirm exactly once: ack=%#v, logs=%d", ack, store.operationCount())
	}
}

func TestRecoveryRejectsSnapshotOlderThanCommittedOperation(t *testing.T) {
	store := newFakeStore("abc")
	store.unknownCommit = true
	r, subs := setupRoom(t, store, "owner", "alice")
	old := store.doc
	store.mu.Lock()
	store.loadOverride = &old // Simulate a stale read paired with the new log.
	store.mu.Unlock()
	sendEdit(t, r, subs["alice"], "uncertain-stale", 0, protocol.Splice{Pos: 1, Text: "X"})
	expectErrorCode(t, subs["alice"], "INTERNAL_ERROR")
	expectNoMessage(t, subs["owner"])
	if _, err := r.Snapshot(context.Background(), "owner"); !errors.Is(err, ErrBusy) {
		t.Fatalf("stale recovery snapshot = %v, want ErrBusy", err)
	}
	store.mu.Lock()
	store.loadOverride = nil
	store.mu.Unlock()
	snap, err := r.Snapshot(context.Background(), "owner")
	if err != nil || snap.Content != "aXbc" || snap.Version != 1 {
		t.Fatalf("reconciled snapshot = %+v, %v", snap, err)
	}
	expectType(t, subs["owner"], "applied")
	expectType(t, subs["alice"], "applied")
}

func TestUnknownPermissionCommitClosesRevokedConnectionAfterReload(t *testing.T) {
	store := newFakeStore("abc")
	store.unknownPermission = true // The none role committed, but COMMIT reply was lost.
	r, subs := setupRoom(t, store, "owner", "alice", "bob")
	store.failReload.Store(true)
	if role, err := r.ChangePermission(context.Background(), "owner", "bob", model.RoleNone); !errors.Is(err, ErrBusy) || role != "" {
		t.Fatalf("uncertain permission result = (%q, %v), want ErrBusy", role, err)
	}
	// Room is paused while it cannot reload the committed permission change.
	sendEdit(t, r, subs["alice"], "paused", 0, protocol.Splice{Pos: 1, Text: "X"})
	expectErrorCode(t, subs["alice"], "SERVER_BUSY")
	if store.operationCount() != 0 {
		t.Fatalf("edit committed while permission state was uncertain: logs=%d", store.operationCount())
	}
	store.failReload.Store(false)
	snap, err := r.Snapshot(context.Background(), "owner")
	if err != nil || snap.Version != 0 || snap.Content != "abc" {
		t.Fatalf("reload failed after permission commit: snapshot=%+v, err=%v", snap, err)
	}
	select {
	case <-subs["bob"].closed:
	case <-time.After(2 * time.Second):
		t.Fatal("revoked old connection remained open after reload")
	}
	if _, err := r.Snapshot(context.Background(), "bob"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("revoked user snapshot = %v, want permission denied", err)
	}
	if err := r.Join(context.Background(), newSubscriber("bob")); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("revoked user rejoin = %v, want permission denied", err)
	}
	sendEdit(t, r, subs["alice"], "after-reload", 0, protocol.Splice{Pos: 1, Text: "X"})
	expectType(t, subs["alice"], "applied")
	expectType(t, subs["owner"], "applied")
	expectNoMessage(t, subs["bob"])
	if store.operationCount() != 1 {
		t.Fatalf("expected one edit after recovery, got %d", store.operationCount())
	}
}
