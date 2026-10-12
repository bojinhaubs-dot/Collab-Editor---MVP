package handler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"collab-mvp/internal/auth"
	"collab-mvp/internal/repository"
	"collab-mvp/internal/room"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

func TestStage2HTTPAndWebSocket(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run the real HTTP/WebSocket/MySQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	repo, err := repository.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.AutoMigrate(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	rooms := room.NewManager(repo)
	server := httptest.NewServer(NewRouter(repo, rooms, auth.Tokens{"stage2-owner-token": "owner"}))
	var documentID string
	var first, second *websocket.Conn
	t.Cleanup(func() {
		if first != nil {
			_ = first.Close()
		}
		if second != nil {
			_ = second.Close()
		}
		rooms.Close()
		server.Close()
		if documentID != "" {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			for _, table := range []string{"operations", "document_members", "documents"} {
				column := "document_id"
				if table == "documents" {
					column = "id"
				}
				if _, err := cleanupDB.ExecContext(cleanupCtx, "DELETE FROM "+table+" WHERE "+column+" = ?", documentID); err != nil {
					t.Errorf("cleanup %s: %v", table, err)
				}
			}
		}
		_ = cleanupDB.Close()
	})

	request, err := http.NewRequest(http.MethodPost, server.URL+"/documents", bytes.NewBufferString(`{"title":"stage2.go","content":"abc"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer stage2-owner-token")
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("POST /documents status = %d, want 201", response.StatusCode)
	}
	var created struct {
		DocumentID string `json:"document_id"`
		Content    string `json:"content"`
		Version    int64  `json:"version"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	documentID = created.DocumentID
	if documentID == "" || created.Content != "abc" || created.Version != 0 {
		t.Fatalf("created document = %+v, want abc/v0", created)
	}

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/documents/" + url.PathEscape(documentID) + "/ws"
	wsHeader := http.Header{"Authorization": []string{"Bearer stage2-owner-token"}}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	first, _, err = dialer.Dial(wsURL, wsHeader)
	if err != nil {
		t.Fatalf("first WS dial: %v", err)
	}
	firstSnapshot := readStage2Message(t, first)
	assertStage2Snapshot(t, firstSnapshot, documentID)
	second, _, err = dialer.Dial(wsURL, wsHeader)
	if err != nil {
		t.Fatalf("second WS dial: %v", err)
	}
	secondSnapshot := readStage2Message(t, second)
	assertStage2Snapshot(t, secondSnapshot, documentID)

	edit := map[string]any{
		"type": "edit", "request_id": "stage2-edit-1", "base_version": 0,
		"op": map[string]any{"pos": 1, "delete_count": 0, "text": "X"},
	}
	if err := first.WriteJSON(edit); err != nil {
		t.Fatal(err)
	}
	for i, conn := range []*websocket.Conn{first, second} {
		applied := readStage2Message(t, conn)
		if applied.Type != "applied" || applied.DocumentID != documentID || applied.RequestID != "stage2-edit-1" || applied.Version != 1 || applied.UserID != "owner" || applied.Op.Pos != 1 || applied.Op.Text != "X" {
			t.Fatalf("WS %d applied = %+v", i+1, applied)
		}
	}
	edit["request_id"] = "stage2-stale-edit"
	edit["op"] = map[string]any{"pos": 2, "delete_count": 0, "text": "Y"}
	if err := second.WriteJSON(edit); err != nil {
		t.Fatal(err)
	}
	conflict := readStage2Message(t, second)
	if conflict.Type != "error" || conflict.RequestID != "stage2-stale-edit" || conflict.Code != "VERSION_CONFLICT" || conflict.CurrentVersion != 1 {
		t.Fatalf("stale edit result = %+v, want VERSION_CONFLICT at v1", conflict)
	}

	getRequest, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/documents/%s", server.URL, url.PathEscape(documentID)), nil)
	if err != nil {
		t.Fatal(err)
	}
	getRequest.Header.Set("Authorization", "Bearer stage2-owner-token")
	getResponse, err := (&http.Client{Timeout: 5 * time.Second}).Do(getRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer getResponse.Body.Close()
	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET /documents/%s status = %d, want 200", documentID, getResponse.StatusCode)
	}
	var current stage2Message
	if err := json.NewDecoder(getResponse.Body).Decode(&current); err != nil {
		t.Fatal(err)
	}
	if current.DocumentID != documentID || current.Content != "aXbc" || current.Version != 1 {
		t.Fatalf("GET snapshot = %+v, want aXbc/v1", current)
	}
}

type stage2Message struct {
	Type           string `json:"type"`
	DocumentID     string `json:"document_id"`
	RequestID      string `json:"request_id"`
	UserID         string `json:"user_id"`
	Content        string `json:"content"`
	Role           string `json:"role"`
	Code           string `json:"code"`
	Version        int64  `json:"version"`
	CurrentVersion int64  `json:"current_version"`
	Op             struct {
		Pos         int64  `json:"pos"`
		DeleteCount int64  `json:"delete_count"`
		Text        string `json:"text"`
	} `json:"op"`
}

func readStage2Message(t *testing.T, conn *websocket.Conn) stage2Message {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var message stage2Message
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	return message
}

func assertStage2Snapshot(t *testing.T, message stage2Message, documentID string) {
	t.Helper()
	if message.Type != "snapshot" || message.DocumentID != documentID || message.Content != "abc" || message.Version != 0 || message.Role != "owner" {
		t.Fatalf("initial WS snapshot = %+v, want abc/v0/owner", message)
	}
}
