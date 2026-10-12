package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"collab-mvp/internal/auth"
	"collab-mvp/internal/model"
	"collab-mvp/internal/room"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

type documentCreator interface {
	CreateDocument(context.Context, string, string, string) (model.Document, error)
}

type Handler struct {
	repo              documentCreator
	rooms             *room.Manager
	tokens            auth.Tokens
	sendQueueCapacity int
	maxWSMessageBytes int64
	maxDocumentBytes  int
}

func NewRouter(repo documentCreator, rooms *room.Manager, tokens auth.Tokens) *gin.Engine {
	h := &Handler{repo: repo, rooms: rooms, tokens: tokens, sendQueueCapacity: positiveEnv("WS_SEND_QUEUE_CAPACITY", 64), maxWSMessageBytes: int64(positiveEnv("WS_MAX_MESSAGE_BYTES", 64<<10)), maxDocumentBytes: positiveEnv("DOCUMENT_MAX_BYTES", 1<<20)}
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.POST("/documents", h.createDocument)
	r.GET("/documents/:id", h.getDocument)
	r.PUT("/documents/:id/members/:user_id", h.changePermission)
	r.GET("/documents/:id/ws", h.websocket)
	return r
}

func positiveEnv(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func (h *Handler) changePermission(c *gin.Context) {
	actor, ok := h.user(c)
	if !ok {
		return
	}
	target := c.Param("user_id")
	known := false
	for _, user := range h.tokens {
		if user == target {
			known = true
			break
		}
	}
	if !known {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_REQUEST", "message": "unknown demo user"})
		return
	}
	var body struct {
		Role string `json:"role"`
	}
	if err := parseJSON(c, &body, 4096); err != nil || (body.Role != model.RoleEditor && body.Role != model.RoleViewer && body.Role != model.RoleNone) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_REQUEST", "message": "invalid role"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	r, err := h.rooms.Get(ctx, c.Param("id"))
	if err != nil {
		h.writeError(c, err)
		return
	}
	role, err := r.ChangePermission(ctx, actor, target, body.Role)
	if err != nil {
		h.writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"document_id": c.Param("id"), "user_id": target, "role": role})
}

type wsClient struct {
	user string
	conn *websocket.Conn
	send chan any
	done chan struct{}
	once sync.Once
}

func (c *wsClient) UserID() string { return c.user }

func (c *wsClient) Closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *wsClient) Send(value any) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case <-c.done:
		return false
	case c.send <- value:
		return true
	default:
		return false
	}
}

func (c *wsClient) Close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *wsClient) writeLoop() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	defer c.Close()
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.send:
			select {
			case <-c.done:
				return
			default:
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.conn.WriteJSON(msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Handler) websocket(c *gin.Context) {
	user, ok := h.user(c)
	if !ok {
		return
	}
	preCtx, preCancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	r, err := h.rooms.Get(preCtx, c.Param("id"))
	if err != nil {
		preCancel()
		h.writeError(c, err)
		return
	}
	// Keep denied users on HTTP 403. Join checks again inside the Room so a
	// permission change between this check and upgrade cannot grant access.
	if _, err := r.Snapshot(preCtx, user); err != nil {
		preCancel()
		h.writeError(c, err)
		return
	}
	preCancel()
	conn, err := (&websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096}).Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	client := &wsClient{user: user, conn: conn, send: make(chan any, h.sendQueueCapacity), done: make(chan struct{})}
	go client.writeLoop()
	defer func() {
		client.Close()
		r.Leave(client)
	}()
	joinCtx, joinCancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	err = r.Join(joinCtx, client)
	joinCancel()
	if err != nil {
		return
	}
	conn.SetReadLimit(h.maxWSMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(60 * time.Second)) })
	for {
		kind, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if kind != websocket.TextMessage {
			if !client.Send(map[string]any{"type": "error", "code": "INVALID_REQUEST"}) {
				return
			}
			continue
		}
		if err := r.Handle(c.Request.Context(), client, raw); err != nil {
			client.Send(map[string]any{"type": "error", "code": "SERVER_BUSY"})
			return
		}
	}
}

func (h *Handler) user(c *gin.Context) (string, bool) {
	user, ok := h.tokens.User(c.Request)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"code": "UNAUTHENTICATED", "message": "valid bearer token required"})
	}
	return user, ok
}

func parseJSON(c *gin.Context, dst any, maxBytes int64) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return err
	}
	if !utf8.Valid(raw) {
		return errors.New("invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("only one JSON value is allowed")
	}
	return nil
}

func (h *Handler) createDocument(c *gin.Context) {
	user, ok := h.user(c)
	if !ok {
		return
	}
	var body struct {
		Title   string `json:"title"`
		Content string `json:"content"`
	}
	if err := parseJSON(c, &body, int64(h.maxDocumentBytes)*8+4096); err != nil || strings.TrimSpace(body.Title) == "" || len(body.Title) > 255 || !utf8.ValidString(body.Content) || len(body.Content) > h.maxDocumentBytes {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_REQUEST", "message": "invalid title or content"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	doc, err := h.repo.CreateDocument(ctx, body.Title, body.Content, user)
	if err != nil {
		h.writeError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"document_id": doc.ID, "content": doc.Content, "version": doc.Version})
}

func (h *Handler) getDocument(c *gin.Context) {
	user, ok := h.user(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	r, err := h.rooms.Get(ctx, c.Param("id"))
	if err != nil {
		h.writeError(c, err)
		return
	}
	snap, err := r.Snapshot(ctx, user)
	if err != nil {
		h.writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"document_id": snap.DocumentID, "content": snap.Content, "version": snap.Version})
}

func (h *Handler) writeError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.JSON(http.StatusNotFound, gin.H{"code": "NOT_FOUND", "message": "document not found"})
	case errors.Is(err, room.ErrPermissionDenied):
		c.JSON(http.StatusForbidden, gin.H{"code": "PERMISSION_DENIED", "message": "permission denied"})
	case errors.Is(err, room.ErrBusy):
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "SERVER_BUSY", "message": "room queue busy"})
	case errors.Is(err, room.ErrInvalidPermission):
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_REQUEST", "message": "invalid permission change"})
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "SERVER_BUSY", "message": "request timed out"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"code": "INTERNAL_ERROR", "message": "request failed"})
	}
}
