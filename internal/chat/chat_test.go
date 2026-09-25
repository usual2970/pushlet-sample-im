package chat

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/usual2970/pushlet"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/store"
)

// Template paths reach the real pages from this package's directory.
// Production embeds the same files via //go:embed in the main package; tests
// parse them from disk so there is exactly one copy to keep honest.
const (
	loginTemplatePath = "../../web/templates/login.html"
	chatTemplatePath  = "../../web/templates/chat.html"
)

// failingPublisher fails every publish with a fixed error; the 502-path test
// injects it in place of the real pushlet.
type failingPublisher struct{ err error }

func (f failingPublisher) PublishJSON(string, string, any) error { return f.err }

// fixture wires a store, the auth service, the chat service, and a real
// local-mode pushlet the way main.go does, over a fresh SQLite file. pub
// overrides the pushlet for the publish-failure test; nil uses the pushlet.
type fixture struct {
	store *store.Store
	ts    *httptest.Server
}

func newFixture(t *testing.T, pub Publisher) *fixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tpl, err := template.ParseFiles(loginTemplatePath, chatTemplatePath)
	if err != nil {
		t.Fatalf("parse templates: %v", err)
	}

	push := pushlet.New()
	if pub == nil {
		pub = push
	}
	push.Start()
	t.Cleanup(push.Stop)

	authSvc := auth.NewService(st, tpl)
	chatSvc := NewService(st, pub, tpl)
	mux := http.NewServeMux()
	mux.HandleFunc("/events", push.HandleSSE)
	mux.HandleFunc("/api/register", authSvc.HandleRegister)
	mux.HandleFunc("/api/login", authSvc.HandleLogin)
	mux.Handle("POST /api/messages", authSvc.RequireAuth(http.HandlerFunc(chatSvc.HandlePostMessage)))
	mux.Handle("GET /api/messages", authSvc.RequireAuth(http.HandlerFunc(chatSvc.HandleListMessages)))
	mux.Handle("/chat", authSvc.RequireAuth(http.HandlerFunc(chatSvc.HandleChatPage)))

	fx := &fixture{store: st, ts: httptest.NewServer(mux)}
	t.Cleanup(fx.ts.Close)
	return fx
}

// session registers username directly in the store and returns its user id
// and a session cookie token. Going through the HTTP register endpoint would
// pay a bcrypt round trip per call; the rows are the ones the middleware
// resolves either way. Each fixture gets a fresh store, so re-registering the
// same name across tests never collides.
func session(t *testing.T, fx *fixture, username string) (userID, token string) {
	t.Helper()
	user, err := fx.store.Register(context.Background(), username, []byte("session-password"))
	if err != nil {
		t.Fatalf("register %s: %v", username, err)
	}
	token, err = fx.store.NewSessionToken()
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	if err := fx.store.CreateSession(context.Background(), token, user.ID); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return user.ID, token
}

// postMessage posts body to /api/messages carrying the session cookie and
// returns the status, the decoded reply, and the decoded error message.
func postMessage(t *testing.T, ts *httptest.Server, token, body string) (int, Message, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/messages",
		strings.NewReader(`{"body":`+quoteJSON(t, body)+`}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	return doMessageRequest(t, ts.Client(), req)
}

// doMessageRequest runs one request against the messages API and decodes
// whichever of the reply payload or error object the body carries.
func doMessageRequest(t *testing.T, client *http.Client, req *http.Request) (int, Message, string) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var msg Message
	_ = json.Unmarshal(raw, &msg)
	var errBody struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &errBody)
	return resp.StatusCode, msg, errBody.Error
}

// quoteJSON marshals s (newlines, unicode and all) into a JSON string token.
func quoteJSON(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return string(b)
}

// getWithCookie fetches urlStr carrying the session cookie, without
// following redirects.
func getWithCookie(t *testing.T, ts *httptest.Server, urlStr, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", urlStr, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// history fetches GET /api/messages with the given after parameter ("" sends
// none) and returns the raw body plus the decoded messages.
func history(t *testing.T, ts *httptest.Server, token, after string) (string, []Message) {
	t.Helper()
	urlStr := ts.URL + "/api/messages"
	if after != "" {
		urlStr += "?after=" + after
	}
	resp := getWithCookie(t, ts, urlStr, token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history status %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	var messages []Message
	if err := json.Unmarshal(raw, &messages); err != nil {
		t.Fatalf("decode history %q: %v", raw, err)
	}
	return string(raw), messages
}

// sseReader reads one Server-Sent Events subscription into parsed events.
type sseReader struct {
	cancel context.CancelFunc
	lines  *bufio.Scanner
}

// subscribe opens an SSE stream on topic and fails the test unless the
// endpoint answers 200 with a text/event-stream body. The stream is bound to
// a 10s context so reads can never hang a failing test.
func subscribe(t *testing.T, ts *httptest.Server, topic string) *sseReader {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events?topic="+topic, nil)
	if err != nil {
		cancel()
		t.Fatalf("new request: %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("subscribe: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("subscribe status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		cancel()
		t.Fatalf("subscribe content type %q, want text/event-stream", ct)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return &sseReader{cancel: cancel, lines: bufio.NewScanner(resp.Body)}
}

// next returns the next event/data pair on the stream, skipping heartbeat
// comments, and fails the test if the stream ends first.
func (s *sseReader) next(t *testing.T) (event, data string) {
	t.Helper()
	got := false
	for s.lines.Scan() {
		line := s.lines.Text()
		switch {
		case strings.HasPrefix(line, ":"):
			// Heartbeat comment; ignore.
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
			got = true
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
			got = true
		case line == "":
			if got {
				return event, data
			}
		}
	}
	if err := s.lines.Err(); err != nil {
		t.Fatalf("sse read: %v", err)
	}
	t.Fatal("sse stream ended before a full event arrived")
	return "", ""
}

// nextMessage skips non-message events until a "message" event arrives and
// decodes its data as a [Message].
func (s *sseReader) nextMessage(t *testing.T) Message {
	t.Helper()
	for {
		event, data := s.next(t)
		if event != "message" {
			t.Logf("skipping event %q while waiting for message", event)
			continue
		}
		var msg Message
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			t.Fatalf("decode message data %q: %v", data, err)
		}
		return msg
	}
}

// TestPostMessageRejectsAnonymous: no session cookie, no posting.
func TestPostMessageRejectsAnonymous(t *testing.T) {
	fx := newFixture(t, nil)

	req, err := http.NewRequest(http.MethodPost, fx.ts.URL+"/api/messages",
		strings.NewReader(`{"body":"hi"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	status, _, errMsg := doMessageRequest(t, fx.ts.Client(), req)
	if status != http.StatusUnauthorized {
		t.Fatalf("anonymous post status %d, want 401", status)
	}
	if errMsg == "" {
		t.Fatal("anonymous post error message is empty")
	}
}

// TestChatPageRedirectsAnonymous: the room page sends visitors to /login.
func TestChatPageRedirectsAnonymous(t *testing.T) {
	fx := newFixture(t, nil)

	resp := getWithCookie(t, fx.ts, fx.ts.URL+"/chat", "")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("anonymous /chat status %d, want 302", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/login" {
		t.Fatalf("anonymous /chat redirects to %q, want /login", loc)
	}
}

// TestBodyValidation pins the 1..2000-byte rule after trimming: empty and
// whitespace-only bodies are rejected with a clear 400, one byte over the cap
// is rejected, and exactly 2000 bytes pass.
func TestBodyValidation(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "validator")

	cases := []struct {
		name    string
		body    string
		want    int
		wantErr string
	}{
		{"empty", "", http.StatusBadRequest, "required"},
		{"whitespace only", " \n\t  \r\n", http.StatusBadRequest, "required"},
		{"one byte over cap", strings.Repeat("a", maxBodyBytes+1), http.StatusBadRequest, "at most"},
		{"exactly at cap", strings.Repeat("a", maxBodyBytes), http.StatusCreated, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, msg, errMsg := postMessage(t, fx.ts, token, tc.body)
			if status != tc.want {
				t.Fatalf("status %d, want %d (error %q)", status, tc.want, errMsg)
			}
			if tc.want == http.StatusBadRequest && !strings.Contains(errMsg, tc.wantErr) {
				t.Fatalf("error %q does not contain %q", errMsg, tc.wantErr)
			}
			if tc.want == http.StatusCreated && tc.body != "" && msg.Body != tc.body {
				t.Fatalf("accepted body round trip: got %d bytes, want %d", len(msg.Body), len(tc.body))
			}
		})
	}

	// Leading and trailing whitespace is trimmed before persisting.
	status, msg, _ := postMessage(t, fx.ts, token, "  padded hello  ")
	if status != http.StatusCreated {
		t.Fatalf("padded body status %d, want 201", status)
	}
	if msg.Body != "padded hello" {
		t.Fatalf("stored body %q, want the trimmed form", msg.Body)
	}

	// Malformed JSON stays a 400, never a 500.
	req, err := http.NewRequest(http.MethodPost, fx.ts.URL+"/api/messages", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	if status, _, _ := doMessageRequest(t, fx.ts.Client(), req); status != http.StatusBadRequest {
		t.Fatalf("malformed body status %d, want 400", status)
	}
}

// TestPostMessagePersistsPublishesAndReplies walks one message through the
// whole surface: a subscribed SSE client receives the published payload, the
// 201 reply carries the same payload, and history reads it back.
func TestPostMessagePersistsPublishesAndReplies(t *testing.T) {
	fx := newFixture(t, nil)
	userID, token := session(t, fx, "poster")

	stream := subscribe(t, fx.ts, "room")
	if event, _ := stream.next(t); event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}

	status, reply, errMsg := postMessage(t, fx.ts, token, "hello room")
	if status != http.StatusCreated {
		t.Fatalf("post status %d (error %q), want 201", status, errMsg)
	}
	if reply.ID == 0 || reply.AuthorID != userID || reply.AuthorName != "poster" || reply.Body != "hello room" {
		t.Fatalf("reply %+v does not carry the server-stamped message", reply)
	}
	if reply.CreatedAt <= 0 {
		t.Fatalf("reply created_at %d, want a unix timestamp", reply.CreatedAt)
	}

	got := stream.nextMessage(t)
	if got != reply {
		t.Fatalf("stream payload %+v, want the posted reply %+v", got, reply)
	}

	_, messages := history(t, fx.ts, token, "")
	if len(messages) != 1 || messages[0] != reply {
		t.Fatalf("history %+v, want the persisted message %+v", messages, reply)
	}
}

// TestMessageNewlinesUnicodeRoundTrip proves multi-line and non-ASCII bodies
// cross the SSE stream without framing corruption: exactly one well-formed
// message event arrives whose JSON payload matches byte for byte.
func TestMessageNewlinesUnicodeRoundTrip(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "polyglot")

	stream := subscribe(t, fx.ts, "room")
	if event, _ := stream.next(t); event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}

	const body = "line one\nline two\n你好 🎉"
	status, reply, errMsg := postMessage(t, fx.ts, token, body)
	if status != http.StatusCreated {
		t.Fatalf("post status %d (error %q), want 201", status, errMsg)
	}
	if reply.Body != body {
		t.Fatalf("reply body %q, want %q", reply.Body, body)
	}

	got := stream.nextMessage(t)
	if got != reply {
		t.Fatalf("stream payload %+v, want %+v", got, reply)
	}

	// The next message event must be the sentinel — a duplicated or
	// re-framed delivery of the first body would surface here instead.
	if status, _, errMsg := postMessage(t, fx.ts, token, "sentinel"); status != http.StatusCreated {
		t.Fatalf("sentinel post status %d (error %q), want 201", status, errMsg)
	}
	if got := stream.nextMessage(t); got.Body != "sentinel" {
		t.Fatalf("event after the round trip is %q, want the sentinel", got.Body)
	}
}

// TestScriptBodyEscapesInRenderedPage: a script-tag body must come back
// HTML-escaped in the server-rendered history (KTD12's server half — the
// client half is the textContent-only rendering in app.js).
func TestScriptBodyEscapesInRenderedPage(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "sneaky")

	status, _, errMsg := postMessage(t, fx.ts, token, "<script>alert(1)</script>")
	if status != http.StatusCreated {
		t.Fatalf("post status %d (error %q), want 201", status, errMsg)
	}

	resp := getWithCookie(t, fx.ts, fx.ts.URL+"/chat", token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/chat status %d, want 200", resp.StatusCode)
	}
	page, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if !strings.Contains(string(page), "&lt;script&gt;") {
		t.Fatal("page does not contain the escaped script body")
	}
	if strings.Contains(string(page), "<script>alert") {
		t.Fatal("page contains an unescaped script body")
	}
}

// TestChatPageRendersShellAndHistory pins the contract app.js builds on:
// server-rendered rows carry data-id attributes and time placeholders, and
// the page ships the composer, the reconnect banner, and the reserved online
// list container.
func TestChatPageRendersShellAndHistory(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "renderer")

	for _, body := range []string{"first post", "second post"} {
		if status, _, errMsg := postMessage(t, fx.ts, token, body); status != http.StatusCreated {
			t.Fatalf("post %q status %d (error %q), want 201", body, status, errMsg)
		}
	}

	resp := getWithCookie(t, fx.ts, fx.ts.URL+"/chat", token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/chat status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("/chat content type %q, want text/html", ct)
	}
	page, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	for _, want := range []string{
		"renderer", // the signed-in user's name
		"first post", "second post",
		`data-id="`,           // rows keyed by server message id
		`data-created-at="`,   // timestamps filled in client-side
		`id="messages"`,       // the message list
		`id="composer"`,       // the composer form
		`id="composer-input"`, // the composer input
		`id="conn-banner"`,    // the reconnecting banner
		`id="online-list"`,    // U4's reserved online list container
		`id="logout"`,         // the logout button
		`id="dm"`,             // U5's direct-message panel
		`id="dm-messages"`,    // the open conversation's message list
		`id="dm-composer"`,    // the DM composer form
		`id="dm-input"`,       // the DM composer input
		`id="dm-list"`,        // the DM conversation list
		"/static/app.js",      // the client script
	} {
		if !strings.Contains(string(page), want) {
			t.Fatalf("page is missing %q", want)
		}
	}
}

// TestHistoryOrderingAfterFilterAndCap seeds 60 messages and checks the
// history endpoint: the most recent 50 come back oldest-first, after=<id>
// narrows to strictly newer messages, and past-the-end reads answer an empty
// JSON array (never null). Bad after values are rejected with 400.
func TestHistoryOrderingAfterFilterAndCap(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "historian")

	ctx := context.Background()
	for i := 1; i <= 60; i++ {
		if _, err := fx.store.AppendMessage(ctx, RoomScope, "u-historian", "historian", "m", int64(1000+i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	_, messages := history(t, fx.ts, token, "")
	if len(messages) != 50 {
		t.Fatalf("history returned %d messages, want the 50 cap", len(messages))
	}
	for i, m := range messages {
		wantID := int64(i + 11) // ids 11..60, oldest first
		if m.ID != wantID {
			t.Fatalf("message %d has id %d, want %d (oldest-first)", i, m.ID, wantID)
		}
	}

	_, newer := history(t, fx.ts, token, "55")
	if len(newer) != 5 || newer[0].ID != 56 || newer[4].ID != 60 {
		t.Fatalf("after=55 returned ids %v, want 56..60", ids(newer))
	}

	raw, none := history(t, fx.ts, token, "60")
	if len(none) != 0 {
		t.Fatalf("after=60 returned %d messages, want none", len(none))
	}
	if strings.TrimSpace(raw) != "[]" {
		t.Fatalf("empty history raw body %q, want []", raw)
	}

	resp := getWithCookie(t, fx.ts, fx.ts.URL+"/api/messages?after=abc", token)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("after=abc status %d, want 400", resp.StatusCode)
	}
}

// ids extracts the id column for failure messages.
func ids(messages []Message) []int64 {
	out := make([]int64, len(messages))
	for i, m := range messages {
		out[i] = m.ID
	}
	return out
}

// TestTwoSubscribersReceivePublishedMessage: one posted message fans out to
// every room subscriber with the author's name, author id, and the server
// timestamp.
func TestTwoSubscribersReceivePublishedMessage(t *testing.T) {
	fx := newFixture(t, nil)
	userID, token := session(t, fx, "broadcaster")

	first := subscribe(t, fx.ts, "room")
	second := subscribe(t, fx.ts, "room")
	for _, s := range []*sseReader{first, second} {
		if event, _ := s.next(t); event != "connected" {
			t.Fatalf("first event %q, want connected", event)
		}
	}

	before := time.Now().Unix()
	status, reply, errMsg := postMessage(t, fx.ts, token, "fan out")
	if status != http.StatusCreated {
		t.Fatalf("post status %d (error %q), want 201", status, errMsg)
	}
	if reply.AuthorID != userID || reply.AuthorName != "broadcaster" {
		t.Fatalf("reply %+v lacks the author identity", reply)
	}
	if reply.CreatedAt < before {
		t.Fatalf("reply created_at %d predates the request %d", reply.CreatedAt, before)
	}

	for name, s := range map[string]*sseReader{"first": first, "second": second} {
		if got := s.nextMessage(t); got != reply {
			t.Fatalf("%s subscriber got %+v, want %+v", name, got, reply)
		}
	}
}

// TestPublishFailureReturns502AndKeepsRow pins the publish-failure policy:
// the POST answers 502 while the row stays in history, because backfill on
// reconnect still delivers it (see the comment in HandlePostMessage).
func TestPublishFailureReturns502AndKeepsRow(t *testing.T) {
	fx := newFixture(t, failingPublisher{err: errors.New("relay is down")})
	_, token := session(t, fx, "unlucky")

	status, _, errMsg := postMessage(t, fx.ts, token, "persisted but not broadcast")
	if status != http.StatusBadGateway {
		t.Fatalf("post status %d, want 502", status)
	}
	if !strings.Contains(errMsg, "not broadcast") {
		t.Fatalf("error %q does not explain the degraded broadcast", errMsg)
	}

	_, messages := history(t, fx.ts, token, "")
	if len(messages) != 1 || messages[0].Body != "persisted but not broadcast" {
		t.Fatalf("history %+v, want the persisted-but-unpublished row", messages)
	}
}

// TestChatPageSurvivesStoreReopen proves the R6/AE5 backfill story at the
// page level: messages live in SQLite, so a fresh service over the same file
// still renders them.
func TestChatPageSurvivesStoreReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	user, err := st.Register(ctx, "survivor", []byte("pw"))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := st.AppendMessage(ctx, RoomScope, user.ID, "survivor", "across restarts", 1234); err != nil {
		t.Fatalf("append: %v", err)
	}
	token, err := st.NewSessionToken()
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	if err := st.CreateSession(ctx, token, user.ID); err != nil {
		t.Fatalf("create session: %v", err)
	}

	tpl, err := template.ParseFiles(loginTemplatePath, chatTemplatePath)
	if err != nil {
		t.Fatalf("parse templates: %v", err)
	}
	push := pushlet.New()
	push.Start()
	t.Cleanup(push.Stop)
	authSvc := auth.NewService(st, tpl)
	chatSvc := NewService(st, push, tpl)
	mux := http.NewServeMux()
	mux.Handle("/chat", authSvc.RequireAuth(http.HandlerFunc(chatSvc.HandleChatPage)))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reChat := NewService(reopened, push, tpl)
	reAuth := auth.NewService(reopened, tpl)
	mux2 := http.NewServeMux()
	mux2.Handle("/chat", reAuth.RequireAuth(http.HandlerFunc(reChat.HandleChatPage)))
	ts2 := httptest.NewServer(mux2)
	t.Cleanup(ts2.Close)

	req, err := http.NewRequest(http.MethodGet, ts2.URL+"/chat", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	resp, err := ts2.Client().Do(req)
	if err != nil {
		t.Fatalf("get /chat: %v", err)
	}
	defer resp.Body.Close()
	page, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if !strings.Contains(string(page), "across restarts") {
		t.Fatalf("reopened page %q lost the persisted message", page)
	}
}
