// Command demo exercises the collaboration service through real HTTP and
// WebSocket requests. Run again with -verify after restarting the server.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"collab-mvp/internal/repository"

	"github.com/gorilla/websocket"
)

type demo struct {
	base    *url.URL
	tokens  map[string]string
	http    *http.Client
	repo    *repository.Repository
	open    []*websocket.Conn
	id      string
	latency []time.Duration
}

type apiMessage struct {
	DocumentID string `json:"document_id"`
	UserID     string `json:"user_id"`
	Content    string `json:"content"`
	Version    int64  `json:"version"`
	Role       string `json:"role"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

type wsMessage struct {
	Type           string `json:"type"`
	DocumentID     string `json:"document_id"`
	UserID         string `json:"user_id"`
	RequestID      string `json:"request_id"`
	Content        string `json:"content"`
	Version        int64  `json:"version"`
	CurrentVersion int64  `json:"current_version"`
	Role           string `json:"role"`
	Code           string `json:"code"`
	Duplicate      bool   `json:"duplicate"`
	Op             struct {
		Pos         int64  `json:"pos"`
		DeleteCount int64  `json:"delete_count"`
		Text        string `json:"text"`
	} `json:"op"`
}

func main() {
	verify := flag.String("verify", "", "document ID to verify after a server restart")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	d, err := newDemo()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *verify == "" {
		err = d.run()
	} else {
		err = d.verify(*verify)
	}
	d.close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "DEMO FAILED:", err)
		os.Exit(1)
	}
}

func newDemo() (*demo, error) {
	base := os.Getenv("BASE_URL")
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	parsed, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("invalid BASE_URL %q", base)
	}
	tokens := make(map[string]string, 4)
	for _, user := range []string{"admin", "alice", "bob", "viewer"} {
		secret := os.Getenv("TOKEN_" + strings.ToUpper(user))
		if secret == "" {
			return nil, fmt.Errorf("TOKEN_%s is required", strings.ToUpper(user))
		}
		tokens[user] = secret
	}
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return nil, errors.New("MYSQL_DSN is required to verify operation counts")
	}
	repo, err := repository.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &demo{base: parsed, tokens: tokens, http: &http.Client{Timeout: 5 * time.Second}, repo: repo}, nil
}

func (d *demo) close() {
	for _, conn := range d.open {
		_ = conn.Close()
	}
}

func (d *demo) endpoint(path string) string {
	u := *d.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	return u.String()
}

func (d *demo) wsEndpoint() string {
	u := *d.base
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/documents/" + url.PathEscape(d.id) + "/ws"
	return u.String()
}

func (d *demo) request(user, method, path string, payload any) (int, apiMessage, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, apiMessage{}, err
		}
		body = bytes.NewReader(encoded)
		fmt.Printf("--> HTTP %s %s as %s %s\n", method, path, user, encoded)
	} else {
		fmt.Printf("--> HTTP %s %s as %s\n", method, path, user)
	}
	req, err := http.NewRequest(method, d.endpoint(path), body)
	if err != nil {
		return 0, apiMessage{}, err
	}
	req.Header.Set("Authorization", "Bearer "+d.tokens[user])
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return 0, apiMessage{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return 0, apiMessage{}, err
	}
	fmt.Printf("<-- HTTP %d %s\n", resp.StatusCode, bytes.TrimSpace(raw))
	var result apiMessage
	if err := json.Unmarshal(raw, &result); err != nil {
		return 0, apiMessage{}, err
	}
	return resp.StatusCode, result, nil
}

func (d *demo) connect(user string) (*websocket.Conn, wsMessage, error) {
	endpoint := d.wsEndpoint()
	fmt.Printf("--> WS CONNECT %s as %s\n", endpoint, user)
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 5 * time.Second}).Dial(endpoint, http.Header{"Authorization": []string{"Bearer " + d.tokens[user]}})
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			fmt.Printf("<-- WS HANDSHAKE %d\n", resp.StatusCode)
		}
		return nil, wsMessage{}, err
	}
	d.open = append(d.open, conn)
	snapshot, err := d.read(conn, user, 5*time.Second)
	return conn, snapshot, err
}

func (d *demo) deniedConnect(user string, wantStatus int) error {
	endpoint := d.wsEndpoint()
	fmt.Printf("--> WS CONNECT %s as %s\n", endpoint, user)
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 5 * time.Second}).Dial(endpoint, http.Header{"Authorization": []string{"Bearer " + d.tokens[user]}})
	if conn != nil {
		_ = conn.Close()
	}
	status := 0
	if resp != nil {
		status = resp.StatusCode
		_ = resp.Body.Close()
	}
	fmt.Printf("<-- WS HANDSHAKE %d error=%v\n", status, err)
	return check(err != nil && status == wantStatus, fmt.Sprintf("%s reconnect rejected with HTTP %d", user, wantStatus))
}

func (d *demo) send(conn *websocket.Conn, user string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	fmt.Printf("--> WS %s %s\n", user, encoded)
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, encoded)
}

func (d *demo) read(conn *websocket.Conn, user string, timeout time.Duration) (wsMessage, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return wsMessage{}, err
	}
	kind, raw, err := conn.ReadMessage()
	if err != nil {
		fmt.Printf("<-- WS %s error=%v\n", user, err)
		return wsMessage{}, err
	}
	fmt.Printf("<-- WS %s %s\n", user, raw)
	if kind != websocket.TextMessage {
		return wsMessage{}, fmt.Errorf("unexpected WS message kind %d", kind)
	}
	var message wsMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return wsMessage{}, err
	}
	return message, nil
}

// A read timeout corrupts a Gorilla connection's read state. Callers close
// this observer immediately and never read it again.
func (d *demo) noBroadcast(conn *websocket.Conn, user string) error {
	_, err := d.read(conn, user, 300*time.Millisecond)
	_ = conn.Close()
	var netErr net.Error
	return check(errors.As(err, &netErr) && netErr.Timeout(), "no applied broadcast (bounded 300 ms read)")
}

func (d *demo) documentPath() string { return "/documents/" + url.PathEscape(d.id) }

func (d *demo) permissionPath(user string) string { return d.documentPath() + "/members/" + user }

func (d *demo) grant(user, role string) error {
	status, body, err := d.request("admin", http.MethodPut, d.permissionPath(user), map[string]any{"role": role})
	if err != nil {
		return err
	}
	return check(status == http.StatusOK && body.UserID == user && body.Role == role, fmt.Sprintf("%s role is %s after HTTP success", user, role))
}

func (d *demo) count(want int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := d.repo.CountOperations(ctx, d.id)
	if err != nil {
		return err
	}
	fmt.Printf("<-- MySQL operations count for %s: %d\n", d.id, got)
	return check(got == want, fmt.Sprintf("operation log count is %d", want))
}

func (d *demo) snapshot(wantContent string, wantVersion int64) error {
	status, body, err := d.request("admin", http.MethodGet, d.documentPath(), nil)
	if err != nil {
		return err
	}
	return check(status == http.StatusOK && body.Content == wantContent && body.Version == wantVersion,
		fmt.Sprintf("document snapshot is %q/v%d", wantContent, wantVersion))
}

func edit(requestID string, baseVersion, pos int64, text string) map[string]any {
	return map[string]any{"type": "edit", "request_id": requestID, "base_version": baseVersion,
		"op": map[string]any{"pos": pos, "delete_count": 0, "text": text}}
}

func check(ok bool, description string) error {
	if !ok {
		fmt.Println("ASSERT FAIL:", description)
		return errors.New(description)
	}
	fmt.Println("ASSERT PASS:", description)
	return nil
}

func (d *demo) run() error {
	fmt.Println("STEP 1 — create abc, grant roles, join through WebSocket")
	status, created, err := d.request("admin", http.MethodPost, "/documents", map[string]any{"title": "demo.go", "content": "abc"})
	if err != nil {
		return err
	}
	if err := check(status == http.StatusCreated && created.DocumentID != "" && created.Content == "abc" && created.Version == 0, "new document is abc/v0"); err != nil {
		return err
	}
	d.id = created.DocumentID
	fmt.Println("DOCUMENT_ID:", d.id)
	for _, grant := range []struct{ user, role string }{{"alice", "editor"}, {"bob", "editor"}, {"viewer", "viewer"}} {
		if err := d.grant(grant.user, grant.role); err != nil {
			return err
		}
	}
	connections := make(map[string]*websocket.Conn)
	for _, entry := range []struct{ user, role string }{{"admin", "owner"}, {"alice", "editor"}, {"bob", "editor"}, {"viewer", "viewer"}} {
		conn, snapshot, err := d.connect(entry.user)
		if err != nil {
			return err
		}
		connections[entry.user] = conn
		if err := check(snapshot.Type == "snapshot" && snapshot.Content == "abc" && snapshot.Version == 0 && snapshot.Role == entry.role, entry.user+" joined with abc/v0 and "+entry.role+" role"); err != nil {
			return err
		}
	}

	fmt.Println("\nSTEP 2 — alice inserts X at code-point position 1")
	firstEdit := edit("alice-001", 0, 1, "X")
	start := time.Now()
	if err := d.send(connections["alice"], "alice", firstEdit); err != nil {
		return err
	}
	aliceApplied, err := d.read(connections["alice"], "alice", 5*time.Second)
	if err != nil {
		return err
	}
	d.latency = append(d.latency, time.Since(start))
	if err := check(aliceApplied.Type == "applied" && aliceApplied.RequestID == "alice-001" && aliceApplied.Version == 1 && aliceApplied.Op.Pos == 1 && aliceApplied.Op.Text == "X", "alice received v1 applied"); err != nil {
		return err
	}
	for _, user := range []string{"admin", "bob", "viewer"} {
		message, err := d.read(connections[user], user, 5*time.Second)
		if err != nil {
			return err
		}
		d.latency = append(d.latency, time.Since(start))
		if err := check(message.Type == "applied" && message.RequestID == "alice-001" && message.Version == 1, user+" received v1 applied"); err != nil {
			return err
		}
	}
	if err := d.snapshot("aXbc", 1); err != nil {
		return err
	}

	fmt.Println("\nSTEP 3 — bob deliberately submits stale v0")
	if err := d.send(connections["bob"], "bob", edit("bob-stale", 0, 2, "Y")); err != nil {
		return err
	}
	conflict, err := d.read(connections["bob"], "bob", 5*time.Second)
	if err != nil {
		return err
	}
	if err := check(conflict.Type == "error" && conflict.Code == "VERSION_CONFLICT" && conflict.CurrentVersion == 1, "stale v0 edit explicitly rejected at v1"); err != nil {
		return err
	}
	if err := d.snapshot("aXbc", 1); err != nil {
		return err
	}
	if err := d.count(1); err != nil {
		return err
	}

	fmt.Println("\nSTEP 4 — bob syncs and manually adjusts this example's position")
	if err := d.send(connections["bob"], "bob", map[string]any{"type": "sync", "request_id": "bob-sync"}); err != nil {
		return err
	}
	synced, err := d.read(connections["bob"], "bob", 5*time.Second)
	if err != nil {
		return err
	}
	if err := check(synced.Type == "snapshot" && synced.RequestID == "bob-sync" && synced.Content == "aXbc" && synced.Version == 1, "bob synced aXbc/v1"); err != nil {
		return err
	}
	fmt.Println("For this specific example, bob explicitly changes position 2 to 3. This is not a general automatic merge algorithm.")
	secondEdit := edit("bob-002", 1, 3, "Y")
	start = time.Now()
	if err := d.send(connections["bob"], "bob", secondEdit); err != nil {
		return err
	}
	bobApplied, err := d.read(connections["bob"], "bob", 5*time.Second)
	if err != nil {
		return err
	}
	d.latency = append(d.latency, time.Since(start))
	if err := check(bobApplied.Type == "applied" && bobApplied.RequestID == "bob-002" && bobApplied.Version == 2 && bobApplied.Op.Pos == 3, "bob received v2 applied"); err != nil {
		return err
	}
	for _, user := range []string{"admin", "alice", "viewer"} {
		message, err := d.read(connections[user], user, 5*time.Second)
		if err != nil {
			return err
		}
		d.latency = append(d.latency, time.Since(start))
		if err := check(message.Type == "applied" && message.RequestID == "bob-002" && message.Version == 2, user+" received v2 applied"); err != nil {
			return err
		}
	}
	if err := d.snapshot("aXbYc", 2); err != nil {
		return err
	}
	if err := d.count(2); err != nil {
		return err
	}

	fmt.Println("\nSTEP 5 — duplicate request returns ack only")
	observer, snap, err := d.connect("admin")
	if err != nil {
		return err
	}
	if err := check(snap.Type == "snapshot" && snap.Content == "aXbYc" && snap.Version == 2, "observer joined at v2"); err != nil {
		return err
	}
	if err := d.send(connections["bob"], "bob", secondEdit); err != nil {
		return err
	}
	ack, err := d.read(connections["bob"], "bob", 5*time.Second)
	if err != nil {
		return err
	}
	if err := check(ack.Type == "ack" && ack.RequestID == "bob-002" && ack.Version == 2 && ack.Duplicate, "duplicate edit received ack without reapplying"); err != nil {
		return err
	}
	if err := d.count(2); err != nil {
		return err
	}
	if err := d.noBroadcast(observer, "admin observer"); err != nil {
		return err
	}

	fmt.Println("\nSTEP 6 — bob is downgraded; his existing WS cannot edit")
	if err := d.grant("bob", "viewer"); err != nil {
		return err
	}
	notice, err := d.read(connections["bob"], "bob", 5*time.Second)
	if err != nil {
		return err
	}
	if err := check(notice.Type == "permission_changed" && notice.Role == "viewer", "bob received viewer notice on original WS"); err != nil {
		return err
	}
	observer, snap, err = d.connect("admin")
	if err != nil {
		return err
	}
	if err := check(snap.Type == "snapshot" && snap.Content == "aXbYc" && snap.Version == 2, "observer joined before denied edit"); err != nil {
		return err
	}
	if err := d.send(connections["bob"], "bob", edit("bob-denied", 2, 0, "Z")); err != nil {
		return err
	}
	denied, err := d.read(connections["bob"], "bob", 5*time.Second)
	if err != nil {
		return err
	}
	if err := check(denied.Type == "error" && denied.Code == "PERMISSION_DENIED", "bob's v2 edit rejected after permission HTTP success"); err != nil {
		return err
	}
	if err := d.snapshot("aXbYc", 2); err != nil {
		return err
	}
	if err := d.count(2); err != nil {
		return err
	}
	if err := d.noBroadcast(observer, "admin observer"); err != nil {
		return err
	}

	fmt.Println("\nSTEP 7 — viewer cannot edit, change permissions, or forge identity")
	if err := d.send(connections["viewer"], "viewer", edit("viewer-denied", 2, 0, "Q")); err != nil {
		return err
	}
	viewerDenied, err := d.read(connections["viewer"], "viewer", 5*time.Second)
	if err != nil {
		return err
	}
	if err := check(viewerDenied.Type == "error" && viewerDenied.Code == "PERMISSION_DENIED", "viewer edit rejected"); err != nil {
		return err
	}
	status, response, err := d.request("viewer", http.MethodPut, d.permissionPath("alice"), map[string]any{"role": "none"})
	if err != nil {
		return err
	}
	if err := check(status == http.StatusForbidden && response.Code == "PERMISSION_DENIED", "viewer permission change rejected"); err != nil {
		return err
	}
	forged := edit("viewer-forged", 2, 0, "Q")
	forged["user_id"] = "admin"
	forged["role"] = "owner"
	if err := d.send(connections["viewer"], "viewer", forged); err != nil {
		return err
	}
	forgedDenied, err := d.read(connections["viewer"], "viewer", 5*time.Second)
	if err != nil {
		return err
	}
	if err := check(forgedDenied.Type == "error" && forgedDenied.Code == "PERMISSION_DENIED", "forged body identity cannot elevate viewer"); err != nil {
		return err
	}
	if err := d.count(2); err != nil {
		return err
	}

	fmt.Println("\nSTEP 8 — bob is revoked; old WS closes and reconnect/read fail")
	if err := d.grant("bob", "none"); err != nil {
		return err
	}
	_, err = d.read(connections["bob"], "bob", 2*time.Second)
	var netErr net.Error
	if err := check(err != nil && !(errors.As(err, &netErr) && netErr.Timeout()), "bob's original WS closed after revoke; sync is no longer available"); err != nil {
		return err
	}
	if err := d.deniedConnect("bob", http.StatusForbidden); err != nil {
		return err
	}
	status, response, err = d.request("bob", http.MethodGet, d.documentPath(), nil)
	if err != nil {
		return err
	}
	if err := check(status == http.StatusForbidden && response.Code == "PERMISSION_DENIED", "bob HTTP read rejected after revoke"); err != nil {
		return err
	}
	if err := d.snapshot("aXbYc", 2); err != nil {
		return err
	}
	if err := d.count(2); err != nil {
		return err
	}

	fmt.Println("\nSTEP 9 — restart the service, then verify the persisted state")
	fmt.Printf("DOCUMENT_ID=%s\n", d.id)
	fmt.Printf("After restarting the service with the same MySQL database, run: go run ./cmd/demo -verify %s\n", d.id)
	d.printLatency()
	return nil
}

func (d *demo) verify(documentID string) error {
	d.id = documentID
	fmt.Println("STEP 9 — verify after server restart; document:", d.id)
	if err := d.snapshot("aXbYc", 2); err != nil {
		return err
	}
	if err := d.count(2); err != nil {
		return err
	}
	for _, entry := range []struct{ user, role string }{{"admin", "owner"}, {"alice", "editor"}, {"viewer", "viewer"}} {
		_, snapshot, err := d.connect(entry.user)
		if err != nil {
			return err
		}
		if err := check(snapshot.Type == "snapshot" && snapshot.Content == "aXbYc" && snapshot.Version == 2 && snapshot.Role == entry.role, entry.user+" rejoined with persisted aXbYc/v2 and "+entry.role+" role"); err != nil {
			return err
		}
	}
	if err := d.deniedConnect("bob", http.StatusForbidden); err != nil {
		return err
	}
	fmt.Println("Restart recovery verified. Two committed operations remain in MySQL.")
	return nil
}

func (d *demo) printLatency() {
	if len(d.latency) == 0 {
		return
	}
	values := append([]time.Duration(nil), d.latency...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	nearest := func(percent int) time.Duration {
		index := (percent*len(values)+99)/100 - 1
		return values[index]
	}
	fmt.Printf("Successful edit send→applied latency: samples=%d, p50=%s, p95=%s (nearest-rank; includes database commit)\n", len(values), nearest(50), nearest(95))
}
