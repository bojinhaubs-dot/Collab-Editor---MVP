package room

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"collab-mvp/internal/model"
	"collab-mvp/internal/protocol"
	"collab-mvp/internal/repository"
	"gorm.io/gorm"
)

var (
	ErrPermissionDenied  = errors.New("permission denied")
	ErrBusy              = errors.New("room queue busy")
	ErrClosed            = errors.New("room closed")
	ErrInvalidPermission = errors.New("invalid permission change")
	ErrInternal          = errors.New("room persistence failed")
)

type Store interface {
	LoadDocument(context.Context, string) (model.Document, map[string]string, error)
	CommitEdit(context.Context, string, int64, string, model.Operation) error
	GetOperation(context.Context, string, string, string) (model.Operation, error)
	ChangePermission(context.Context, string, string, string) error
}

type Snapshot struct {
	Type       string `json:"type,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	DocumentID string `json:"document_id"`
	Content    string `json:"content"`
	Version    int64  `json:"version"`
	Role       string `json:"role"`
}

// Subscriber is implemented by a WebSocket connection. Send must never wait
// for a network write; false means the connection is closed or its queue is full.
type Subscriber interface {
	UserID() string
	Send(any) bool
	Closed() bool
	Close()
}

type joinCommand struct {
	client Subscriber
	reply  chan error
}

type leaveCommand struct{ client Subscriber }

type messageCommand struct {
	client Subscriber
	raw    []byte
}

type permissionCommand struct {
	actor  string
	target string
	role   string
	reply  chan permissionResult
}

type permissionResult struct {
	role string
	err  error
}

type pendingEdit struct {
	op  model.Operation
	msg protocol.Splice
}

type pendingPermission struct {
	target string
	role   string
}

type snapshotCommand struct {
	user  string
	reply chan snapshotResult
}

type snapshotResult struct {
	snapshot Snapshot
	err      error
}

// A Room owns its document state. Every access to mutable state occurs in run.
type Room struct {
	doc               model.Document
	members           map[string]string
	clients           map[Subscriber]struct{}
	store             Store
	ctx               context.Context
	paused            bool
	pending           *pendingEdit
	pendingPermission *pendingPermission
	recoveredPending  bool
	maxDocumentBytes  int
	commands          chan any
	done              chan struct{}
}

type Manager struct {
	mu               sync.RWMutex
	rooms            map[string]*Room
	store            Store
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	queueCapacity    int
	maxDocumentBytes int
}

func NewManager(store Store) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{rooms: make(map[string]*Room), store: store, ctx: ctx, cancel: cancel, queueCapacity: positiveEnv("ROOM_QUEUE_CAPACITY", 256), maxDocumentBytes: positiveEnv("DOCUMENT_MAX_BYTES", 1<<20)}
}

func positiveEnv(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func (m *Manager) Get(ctx context.Context, documentID string) (*Room, error) {
	m.mu.RLock()
	if m.ctx.Err() != nil {
		m.mu.RUnlock()
		return nil, ErrClosed
	}
	if r := m.rooms[documentID]; r != nil {
		m.mu.RUnlock()
		return r, nil
	}
	m.mu.RUnlock()
	doc, members, err := m.store.LoadDocument(ctx, documentID)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if r := m.rooms[documentID]; r != nil {
		return r, nil
	}
	r := &Room{doc: doc, members: members, clients: make(map[Subscriber]struct{}), store: m.store, ctx: m.ctx, commands: make(chan any, m.queueCapacity), done: make(chan struct{}), maxDocumentBytes: m.maxDocumentBytes}
	m.rooms[documentID] = r
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		r.run(m.ctx)
	}()
	return r, nil
}

func (m *Manager) Close() {
	m.mu.Lock()
	m.cancel()
	m.mu.Unlock()
	m.wg.Wait()
}

func (r *Room) enqueue(ctx context.Context, cmd any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrClosed
	default:
	}
	select {
	case r.commands <- cmd:
		return nil
	default:
		return ErrBusy
	}
}

func (r *Room) Snapshot(ctx context.Context, user string) (Snapshot, error) {
	reply := make(chan snapshotResult, 1)
	if err := r.enqueue(ctx, snapshotCommand{user: user, reply: reply}); err != nil {
		return Snapshot{}, err
	}
	select {
	case result := <-reply:
		return result.snapshot, result.err
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	case <-r.done:
		return Snapshot{}, ErrClosed
	}
}

func (r *Room) ChangePermission(ctx context.Context, actor, target, role string) (string, error) {
	reply := make(chan permissionResult, 1)
	if err := r.enqueue(ctx, permissionCommand{actor: actor, target: target, role: role, reply: reply}); err != nil {
		return "", err
	}
	select {
	case result := <-reply:
		return result.role, result.err
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.done:
		return "", ErrClosed
	}
}

// Join enqueues the snapshot before subscribing. The Room event loop makes
// that ordering atomic with respect to all subsequent applied events.
func (r *Room) Join(ctx context.Context, client Subscriber) error {
	reply := make(chan error, 1)
	if err := r.enqueue(ctx, joinCommand{client: client, reply: reply}); err != nil {
		return err
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrClosed
	}
}

func (r *Room) Leave(client Subscriber) {
	select {
	case r.commands <- leaveCommand{client: client}:
	case <-r.done:
	default:
		// The connection is already closed. A full command queue must not
		// strand its handler goroutine; run also prunes closed subscribers.
	}
}

func (r *Room) Handle(ctx context.Context, client Subscriber, raw []byte) error {
	return r.enqueue(ctx, messageCommand{client: client, raw: raw})
}

func (r *Room) role(user string) string {
	if user == r.doc.OwnerID {
		return "owner"
	}
	return r.members[user]
}

func (r *Room) snapshot(user string) (Snapshot, error) {
	role := r.role(user)
	if role == "" || role == "none" {
		return Snapshot{}, ErrPermissionDenied
	}
	return Snapshot{DocumentID: r.doc.ID, Content: r.doc.Content, Version: r.doc.Version, Role: role}, nil
}

func (r *Room) send(client Subscriber, value any) {
	if client.Closed() || !client.Send(value) {
		delete(r.clients, client)
		client.Close()
	}
}

func (r *Room) broadcast(value any) {
	for client := range r.clients {
		role := r.role(client.UserID())
		if role == "" || role == model.RoleNone {
			delete(r.clients, client)
			client.Close()
			continue
		}
		r.send(client, value)
	}
}

func (r *Room) fail(client Subscriber, requestID, code string, currentVersion *int64) {
	msg := map[string]any{"type": "error", "request_id": requestID, "code": code}
	if currentVersion != nil {
		msg["current_version"] = *currentVersion
	}
	r.send(client, msg)
}

func (r *Room) handleMessage(cmd messageCommand) {
	if _, joined := r.clients[cmd.client]; !joined {
		return
	}
	if !r.ensureReady() {
		r.fail(cmd.client, "", "SERVER_BUSY", nil)
		return
	}
	var envelope struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(cmd.raw, &envelope); err != nil {
		r.fail(cmd.client, "", "INVALID_REQUEST", nil)
		return
	}
	role := r.role(cmd.client.UserID())
	switch envelope.Type {
	case "edit":
		if role != model.RoleOwner && role != model.RoleEditor {
			r.fail(cmd.client, envelope.RequestID, "PERMISSION_DENIED", nil)
			return
		}
		message, err := protocol.DecodeClient(cmd.raw)
		if err != nil {
			r.fail(cmd.client, envelope.RequestID, "INVALID_REQUEST", nil)
			return
		}
		r.edit(cmd.client, message)
	case "sync":
		if role == "" || role == model.RoleNone {
			r.fail(cmd.client, envelope.RequestID, "PERMISSION_DENIED", nil)
			return
		}
		message, err := protocol.DecodeClient(cmd.raw)
		if err != nil {
			r.fail(cmd.client, envelope.RequestID, "INVALID_REQUEST", nil)
			return
		}
		snap, _ := r.snapshot(cmd.client.UserID())
		snap.Type = "snapshot"
		snap.RequestID = message.RequestID
		r.send(cmd.client, snap)
	default:
		r.fail(cmd.client, envelope.RequestID, "INVALID_REQUEST", nil)
	}
}

func (r *Room) edit(client Subscriber, msg protocol.ClientMessage) {
	hash := protocol.PayloadHash(msg.BaseVersion, msg.Op)
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	previous, err := r.store.GetOperation(ctx, r.doc.ID, client.UserID(), msg.RequestID)
	cancel()
	if err == nil {
		if previous.PayloadHash != hash {
			r.fail(client, msg.RequestID, "REQUEST_ID_REUSED", nil)
		} else {
			r.send(client, map[string]any{"type": "ack", "request_id": msg.RequestID, "version": previous.Version, "duplicate": true})
		}
		return
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		r.fail(client, msg.RequestID, "INTERNAL_ERROR", nil)
		return
	}
	if msg.BaseVersion != r.doc.Version {
		version := r.doc.Version
		r.fail(client, msg.RequestID, "VERSION_CONFLICT", &version)
		return
	}
	if !utf8.ValidString(msg.Op.Text) || msg.Op.Pos < 0 || msg.Op.DeleteCount < 0 {
		r.fail(client, msg.RequestID, "INVALID_OPERATION", nil)
		return
	}
	runes := []rune(r.doc.Content)
	if msg.Op.Pos > int64(len(runes)) || msg.Op.DeleteCount > int64(len(runes))-msg.Op.Pos {
		r.fail(client, msg.RequestID, "INVALID_OPERATION", nil)
		return
	}
	pos := int(msg.Op.Pos)
	end := pos + int(msg.Op.DeleteCount)
	next := string(runes[:pos]) + msg.Op.Text + string(runes[end:])
	if len(next) > r.maxDocumentBytes {
		r.fail(client, msg.RequestID, "INVALID_OPERATION", nil)
		return
	}
	op := model.Operation{DocumentID: r.doc.ID, UserID: client.UserID(), RequestID: msg.RequestID, BaseVersion: msg.BaseVersion, Version: r.doc.Version + 1, Pos: msg.Op.Pos, DeleteCount: msg.Op.DeleteCount, Text: msg.Op.Text, PayloadHash: hash}
	ctx, cancel = context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	if err := r.store.CommitEdit(ctx, r.doc.ID, r.doc.Version, next, op); err != nil {
		if errors.Is(err, repository.ErrCommitUnknown) {
			r.paused = true
			r.pending = &pendingEdit{op: op, msg: msg.Op}
			if !r.reconcile() {
				r.fail(client, msg.RequestID, "INTERNAL_ERROR", nil)
			} else if !r.recoveredPending {
				r.fail(client, msg.RequestID, "INTERNAL_ERROR", nil)
			}
		} else if errors.Is(err, repository.ErrVersionConflict) {
			version := r.doc.Version
			r.fail(client, msg.RequestID, "VERSION_CONFLICT", &version)
		} else {
			r.fail(client, msg.RequestID, "INTERNAL_ERROR", nil)
		}
		return
	}
	r.doc.Content = next
	r.doc.Version++
	r.broadcast(map[string]any{"type": "applied", "document_id": r.doc.ID, "user_id": client.UserID(), "request_id": msg.RequestID, "version": r.doc.Version, "op": msg.Op})
}

func (r *Room) notifyPermission(target, role string) {
	for client := range r.clients {
		if client.UserID() != target {
			continue
		}
		if role == model.RoleNone {
			delete(r.clients, client)
			client.Close()
		} else {
			r.send(client, map[string]any{"type": "permission_changed", "role": role})
		}
	}
}

func (r *Room) changePermission(cmd permissionCommand) {
	if !r.ensureReady() {
		cmd.reply <- permissionResult{err: ErrBusy}
		return
	}
	if r.role(cmd.actor) != model.RoleOwner {
		cmd.reply <- permissionResult{err: ErrPermissionDenied}
		return
	}
	if cmd.target == r.doc.OwnerID || (cmd.role != model.RoleEditor && cmd.role != model.RoleViewer && cmd.role != model.RoleNone) {
		cmd.reply <- permissionResult{err: ErrInvalidPermission}
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	err := r.store.ChangePermission(ctx, r.doc.ID, cmd.target, cmd.role)
	cancel()
	if err != nil {
		if errors.Is(err, repository.ErrCommitUnknown) {
			r.paused = true
			r.pendingPermission = &pendingPermission{target: cmd.target, role: cmd.role}
			if !r.reconcile() {
				cmd.reply <- permissionResult{err: ErrBusy}
				return
			}
			if r.role(cmd.target) == cmd.role {
				cmd.reply <- permissionResult{role: cmd.role}
				return
			}
		}
		cmd.reply <- permissionResult{err: ErrInternal}
		return
	}
	r.members[cmd.target] = cmd.role
	r.notifyPermission(cmd.target, cmd.role)
	cmd.reply <- permissionResult{role: cmd.role}
}

// A failed COMMIT can leave the caller unsure whether MySQL committed. While
// paused, no command may observe or mutate the possibly stale Room state.
// Reconciliation checks the request ID and reloads the committed snapshot.
func (r *Room) reconcile() bool {
	r.recoveredPending = false
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	// The repository's locking load waits for any in-flight uncertain COMMIT.
	// Do not release the pause until the snapshot can account for a found op.
	doc, members, err := r.store.LoadDocument(ctx, r.doc.ID)
	if err != nil {
		return false
	}
	var committed *model.Operation
	if r.pending != nil {
		op, err := r.store.GetOperation(ctx, r.doc.ID, r.pending.op.UserID, r.pending.op.RequestID)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return false
		}
		if err == nil && op.PayloadHash == r.pending.op.PayloadHash {
			committed = &op
		}
	}
	if committed != nil && doc.Version < committed.Version {
		return false
	}
	r.doc = doc
	r.members = members
	if r.pendingPermission != nil && r.role(r.pendingPermission.target) == r.pendingPermission.role {
		r.notifyPermission(r.pendingPermission.target, r.pendingPermission.role)
	}
	if committed != nil {
		r.recoveredPending = true
		r.broadcast(map[string]any{"type": "applied", "document_id": r.doc.ID, "user_id": committed.UserID, "request_id": committed.RequestID, "version": committed.Version, "op": r.pending.msg})
	}
	r.pending = nil
	r.pendingPermission = nil
	r.paused = false
	return true
}

func (r *Room) ensureReady() bool {
	return !r.paused || r.reconcile()
}

func (r *Room) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer func() {
		for client := range r.clients {
			client.Close()
		}
		close(r.done)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for client := range r.clients {
				if client.Closed() {
					delete(r.clients, client)
				}
			}
		case command := <-r.commands:
			switch cmd := command.(type) {
			case snapshotCommand:
				if !r.ensureReady() {
					cmd.reply <- snapshotResult{err: ErrBusy}
					continue
				}
				snap, err := r.snapshot(cmd.user)
				cmd.reply <- snapshotResult{snapshot: snap, err: err}
			case joinCommand:
				if !r.ensureReady() {
					cmd.reply <- ErrBusy
					continue
				}
				snap, err := r.snapshot(cmd.client.UserID())
				if err == nil {
					snap.Type = "snapshot"
					if !cmd.client.Send(snap) {
						err = ErrClosed
						cmd.client.Close()
					} else {
						r.clients[cmd.client] = struct{}{}
					}
				}
				cmd.reply <- err
			case leaveCommand:
				delete(r.clients, cmd.client)
			case messageCommand:
				r.handleMessage(cmd)
			case permissionCommand:
				r.changePermission(cmd)
			}
		}
	}
}
