package handler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
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

func TestStage3PermissionsAndIdempotency(t *testing.T) {
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
	tokens := auth.Tokens{
		"stage3-owner-token":  "owner",
		"stage3-alice-token":  "alice",
		"stage3-bob-token":    "bob",
		"stage3-viewer-token": "viewer",
	}
	server := httptest.NewServer(NewRouter(repo, rooms, tokens))
	var documentID string
	var connections []*websocket.Conn
	t.Cleanup(func() {
		for _, conn := range connections {
			_ = conn.Close()
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

	client := &http.Client{Timeout: 5 * time.Second}
	created := stage3HTTP(t, client, http.MethodPost, server.URL+"/documents", "stage3-owner-token", map[string]any{"title": "stage3.go", "content": "abc"})
	if created.Status != http.StatusCreated || created.Body.DocumentID == "" || created.Body.Content != "abc" || created.Body.Version != 0 {
		t.Fatalf("create response = %+v, want abc/v0", created)
	}
	documentID = created.Body.DocumentID
	documentURL := server.URL + "/documents/" + url.PathEscape(documentID)
	permissionURL := func(target string) string { return documentURL + "/members/" + target }
	for _, grant := range []struct{ user, role string }{{"alice", "editor"}, {"bob", "editor"}, {"viewer", "viewer"}} {
		result := stage3HTTP(t, client, http.MethodPut, permissionURL(grant.user), "stage3-owner-token", map[string]any{"role": grant.role})
		if result.Status != http.StatusOK || result.Body.Role != grant.role || result.Body.UserID != grant.user {
			t.Fatalf("grant %s:%s = %+v", grant.user, grant.role, result)
		}
	}

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/documents/" + url.PathEscape(documentID) + "/ws"
	connect := func(token, role string) *websocket.Conn {
		t.Helper()
		conn, response, err := (&websocket.Dialer{HandshakeTimeout: 5 * time.Second}).Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + token}})
		if err != nil {
			status := 0
			if response != nil {
				status = response.StatusCode
			}
			t.Fatalf("WS dial for %s: status=%d err=%v", role, status, err)
		}
		connections = append(connections, conn)
		snapshot := readStage3Message(t, conn)
		if snapshot.Type != "snapshot" || snapshot.DocumentID != documentID || snapshot.Content != "abc" || snapshot.Version != 0 || snapshot.Role != role {
			t.Fatalf("WS %s initial snapshot = %+v", role, snapshot)
		}
		return conn
	}
	ownerWS := connect("stage3-owner-token", "owner")
	aliceWS := connect("stage3-alice-token", "editor")
	bobWS1 := connect("stage3-bob-token", "editor")
	bobWS2 := connect("stage3-bob-token", "editor")
	viewerWS := connect("stage3-viewer-token", "viewer")

	firstEdit := stage3Edit("stage3-edit-1", 0, 1, "X")
	if err := aliceWS.WriteJSON(firstEdit); err != nil {
		t.Fatal(err)
	}
	for _, conn := range []*websocket.Conn{ownerWS, aliceWS, bobWS1, bobWS2, viewerWS} {
		applied := readStage3Message(t, conn)
		if applied.Type != "applied" || applied.RequestID != "stage3-edit-1" || applied.Version != 1 || applied.UserID != "alice" || applied.Op.Pos != 1 || applied.Op.Text != "X" {
			t.Fatalf("applied event = %+v", applied)
		}
	}
	assertStage3Count(t, repo, documentID, 1)
	if err := aliceWS.WriteJSON(firstEdit); err != nil {
		t.Fatal(err)
	}
	duplicate := readStage3Message(t, aliceWS)
	if duplicate.Type != "ack" || duplicate.RequestID != "stage3-edit-1" || duplicate.Version != 1 || !duplicate.Duplicate {
		t.Fatalf("duplicate result = %+v", duplicate)
	}
	assertStage3Count(t, repo, documentID, 1)
	if err := aliceWS.WriteJSON(stage3Edit("stage3-edit-1", 0, 2, "Y")); err != nil {
		t.Fatal(err)
	}
	reused := readStage3Message(t, aliceWS)
	if reused.Type != "error" || reused.RequestID != "stage3-edit-1" || reused.Code != "REQUEST_ID_REUSED" {
		t.Fatalf("reused request ID result = %+v", reused)
	}
	assertStage3Count(t, repo, documentID, 1)

	downgrade := stage3HTTP(t, client, http.MethodPut, permissionURL("bob"), "stage3-owner-token", map[string]any{"role": "viewer"})
	if downgrade.Status != http.StatusOK || downgrade.Body.Role != "viewer" {
		t.Fatalf("bob downgrade response = %+v", downgrade)
	}
	for _, conn := range []*websocket.Conn{bobWS1, bobWS2} {
		notice := readStage3Message(t, conn)
		if notice.Type != "permission_changed" || notice.Role != "viewer" {
			t.Fatalf("bob permission notice = %+v", notice)
		}
	}
	if err := bobWS1.WriteJSON(stage3Edit("stage3-bob-denied", 1, 2, "Y")); err != nil {
		t.Fatal(err)
	}
	denied := readStage3Message(t, bobWS1)
	if denied.Type != "error" || denied.RequestID != "stage3-bob-denied" || denied.Code != "PERMISSION_DENIED" {
		t.Fatalf("bob edit after successful downgrade = %+v", denied)
	}
	assertStage3Count(t, repo, documentID, 1)
	current := stage3HTTP(t, client, http.MethodGet, documentURL, "stage3-owner-token", nil)
	if current.Status != http.StatusOK || current.Body.Content != "aXbc" || current.Body.Version != 1 {
		t.Fatalf("document after denied edit = %+v", current)
	}
	// The denial has already been processed; any erroneous applied broadcast
	// would be queued on this owner connection. A timeout bounds the negative check.
	if err := ownerWS.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var unexpected stage3Message
	if err := ownerWS.ReadJSON(&unexpected); err == nil {
		t.Fatalf("unexpected broadcast after denied edit: %+v", unexpected)
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("owner WS read error = %v, want bounded timeout", err)
	}
	_ = ownerWS.Close() // A timed-out Gorilla connection must not be read again.

	if err := viewerWS.WriteJSON(stage3Edit("stage3-viewer-denied", 1, 2, "Q")); err != nil {
		t.Fatal(err)
	}
	viewerDenied := readStage3Message(t, viewerWS)
	if viewerDenied.Type != "error" || viewerDenied.Code != "PERMISSION_DENIED" {
		t.Fatalf("viewer edit = %+v", viewerDenied)
	}
	viewerPermission := stage3HTTP(t, client, http.MethodPut, permissionURL("alice"), "stage3-viewer-token", map[string]any{"role": "none"})
	if viewerPermission.Status != http.StatusForbidden || viewerPermission.Body.Code != "PERMISSION_DENIED" {
		t.Fatalf("viewer permission change = %+v", viewerPermission)
	}
	forged := stage3Edit("stage3-forged", 1, 2, "Q")
	forged["user_id"] = "owner"
	forged["role"] = "owner"
	if err := viewerWS.WriteJSON(forged); err != nil {
		t.Fatal(err)
	}
	forgedDenied := readStage3Message(t, viewerWS)
	if forgedDenied.Type != "error" || forgedDenied.Code != "PERMISSION_DENIED" {
		t.Fatalf("forged WS identity result = %+v", forgedDenied)
	}

	revoke := stage3HTTP(t, client, http.MethodPut, permissionURL("bob"), "stage3-owner-token", map[string]any{"role": "none"})
	if revoke.Status != http.StatusOK || revoke.Body.Role != "none" {
		t.Fatalf("bob revoke response = %+v", revoke)
	}
	for _, conn := range []*websocket.Conn{bobWS1, bobWS2} {
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var message stage3Message
		if err := conn.ReadJSON(&message); err == nil {
			t.Fatalf("bob WS stayed open after revoke, received %+v", message)
		} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			t.Fatalf("bob WS did not close after revoke: %v", err)
		}
	}
	reconnect, handshake, err := (&websocket.Dialer{HandshakeTimeout: 5 * time.Second}).Dial(wsURL, http.Header{"Authorization": []string{"Bearer stage3-bob-token"}})
	if reconnect != nil {
		_ = reconnect.Close()
	}
	if handshake != nil && handshake.Body != nil {
		defer handshake.Body.Close()
	}
	if err == nil || handshake == nil || handshake.StatusCode != http.StatusForbidden {
		status := 0
		if handshake != nil {
			status = handshake.StatusCode
		}
		t.Fatalf("bob reconnect: status=%d err=%v, want HTTP 403", status, err)
	}
	bobGET := stage3HTTP(t, client, http.MethodGet, documentURL, "stage3-bob-token", nil)
	if bobGET.Status != http.StatusForbidden || bobGET.Body.Code != "PERMISSION_DENIED" {
		t.Fatalf("bob GET after revoke = %+v", bobGET)
	}
	assertStage3Count(t, repo, documentID, 1)
	final := stage3HTTP(t, client, http.MethodGet, documentURL, "stage3-owner-token", nil)
	if final.Status != http.StatusOK || final.Body.Content != "aXbc" || final.Body.Version != 1 {
		t.Fatalf("final document = %+v", final)
	}
}

type stage3HTTPResult struct {
	Status int
	Body   struct {
		DocumentID string `json:"document_id"`
		UserID     string `json:"user_id"`
		Role       string `json:"role"`
		Content    string `json:"content"`
		Version    int64  `json:"version"`
		Code       string `json:"code"`
	}
}

func stage3HTTP(t *testing.T, client *http.Client, method, endpoint, token string, payload any) stage3HTTPResult {
	t.Helper()
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	result := stage3HTTPResult{Status: response.StatusCode}
	if err := json.NewDecoder(response.Body).Decode(&result.Body); err != nil {
		t.Fatalf("%s %s: decode response: %v", method, endpoint, err)
	}
	return result
}

type stage3Message struct {
	Type       string `json:"type"`
	DocumentID string `json:"document_id"`
	RequestID  string `json:"request_id"`
	UserID     string `json:"user_id"`
	Content    string `json:"content"`
	Role       string `json:"role"`
	Code       string `json:"code"`
	Version    int64  `json:"version"`
	Duplicate  bool   `json:"duplicate"`
	Op         struct {
		Pos         int64  `json:"pos"`
		DeleteCount int64  `json:"delete_count"`
		Text        string `json:"text"`
	} `json:"op"`
}

func readStage3Message(t *testing.T, conn *websocket.Conn) stage3Message {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var message stage3Message
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	return message
}

func stage3Edit(requestID string, baseVersion, pos int64, text string) map[string]any {
	return map[string]any{
		"type": "edit", "request_id": requestID, "base_version": baseVersion,
		"op": map[string]any{"pos": pos, "delete_count": 0, "text": text},
	}
}

func assertStage3Count(t *testing.T, repo *repository.Repository, documentID string, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := repo.CountOperations(ctx, documentID)
	if err != nil || got != want {
		t.Fatalf("operation count = %d, err=%v, want %d", got, err, want)
	}
}
