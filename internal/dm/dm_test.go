package dm

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

// failingPublisher fails every publish with a fixed error; the 502-path test
// injects it in place of the real pushlet.
type failingPublisher struct{ err error }

func (f failingPublisher) PublishJSON(string, string, any) error { return f.err }

// fakePresence stands in for the presence engine: tests flip ids online so
// the 404/409 branches run without the whole presence stack.
type fakePresence struct{ online map[string]bool }

func (f *fakePresence) IsOnline(userID string) bool { return f.online[userID] }

// quietLogger discards pushlet connection logs: the fixture's topics are
// throwaway dm topics, and they belong in test output as little as in logs.
type quietLogger struct{}

func (quietLogger) Println(...any)                       {}
func (quietLogger) WithField(string, any) pushlet.Logger { return quietLogger{} }

// fixture wires a store, the auth middleware, the dm service, and a real
// local-mode pushlet the way main.go does, over a fresh SQLite file. pub
// overrides the pushlet for the publish-failure test; nil uses the pushlet.
type fixture struct {
	store    *store.Store
	push     *pushlet.Pushlet
	ts       *httptest.Server
	presence *fakePresence
}

func newFixture(t *testing.T, pub Publisher) *fixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	push := pushlet.New(pushlet.WithLogger(func() pushlet.Logger { return quietLogger{} }))
	if pub == nil {
		pub = push
	}
	push.Start()
	t.Cleanup(push.Stop)

	pres := &fakePresence{online: map[string]bool{}}
	authSvc := auth.NewService(st, nil)
	dmSvc := NewService(st, pub, pres)
	mux := http.NewServeMux()
	mux.HandleFunc("/events", push.HandleSSE)
	mux.Handle("POST /api/dm", authSvc.RequireAuth(http.HandlerFunc(dmSvc.HandleSend)))
	mux.Handle("GET /api/dm", authSvc.RequireAuth(http.HandlerFunc(dmSvc.HandleHistory)))
	mux.Handle("GET /api/users/{id}", authSvc.RequireAuth(http.HandlerFunc(dmSvc.HandleUser)))

	fx := &fixture{store: st, push: push, ts: httptest.NewServer(mux), presence: pres}
	t.Cleanup(fx.ts.Close)
	return fx
}

// session registers username directly in the store and returns the full user
// record (id and dm secret included) plus a session cookie token — the same
// shortcut the chat tests use to skip the bcrypt round trip.
func session(t *testing.T, fx *fixture, username string) (*store.User, string) {
	t.Helper()
	user, err := fx.store.Register(context.Background(), username, []byte("session-password"))
	if err != nil {
		t.Fatalf("register %s: %v", username, err)
	}
	token, err := fx.store.NewSessionToken()
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	if err := fx.store.CreateSession(context.Background(), token, user.ID); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return user, token
}

// sendDM posts {to, body} to /api/dm carrying the session cookie and returns
// the status, the decoded reply, and the decoded error message.
func sendDM(t *testing.T, ts *httptest.Server, token, to, body string) (int, Message, string) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"to": to, "body": body})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/dm", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("post /api/dm: %v", err)
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

// dmHistory fetches GET /api/dm?with=<id> and returns the status plus the
// decoded messages (nil for error-status bodies).
func dmHistory(t *testing.T, ts *httptest.Server, token, with string) (int, []Message) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/dm?with="+with, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("get /api/dm: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var messages []Message
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &messages); err != nil {
			t.Fatalf("decode history %q: %v", raw, err)
		}
	}
	return resp.StatusCode, messages
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
		if event != messageEvent {
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

// TestDMRoutesRejectAnonymous: no session cookie, no sending or reading.
func TestDMRoutesRejectAnonymous(t *testing.T) {
	fx := newFixture(t, nil)

	for _, method := range []string{http.MethodPost, http.MethodGet} {
		req, err := http.NewRequest(method, fx.ts.URL+"/api/dm", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := fx.ts.Client().Do(req)
		if err != nil {
			t.Fatalf("%s /api/dm: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("anonymous %s /api/dm status %d, want 401", method, resp.StatusCode)
		}
	}

	resp, err := http.Get(fx.ts.URL + "/api/users/whoever")
	if err != nil {
		t.Fatalf("get /api/users: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous GET /api/users/<id> status %d, want 401", resp.StatusCode)
	}
}

// TestSendBodyValidation pins the same 1..2000-byte rule the room enforces,
// applied after trimming, plus the recipient-field requirements.
func TestSendBodyValidation(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "validator")
	recipient, _ := session(t, fx, "target")
	fx.presence.online[recipient.ID] = true

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
			status, msg, errMsg := sendDM(t, fx.ts, token, recipient.ID, tc.body)
			if status != tc.want {
				t.Fatalf("status %d, want %d (error %q)", status, tc.want, errMsg)
			}
			if tc.want == http.StatusBadRequest && !strings.Contains(errMsg, tc.wantErr) {
				t.Fatalf("error %q does not contain %q", errMsg, tc.wantErr)
			}
			if tc.want == http.StatusCreated && len(msg.Body) != len(tc.body) {
				t.Fatalf("accepted body round trip: got %d bytes, want %d", len(msg.Body), len(tc.body))
			}
		})
	}

	// Leading and trailing whitespace is trimmed before persisting.
	status, msg, _ := sendDM(t, fx.ts, token, recipient.ID, "  padded dm  ")
	if status != http.StatusCreated {
		t.Fatalf("padded body status %d, want 201", status)
	}
	if msg.Body != "padded dm" {
		t.Fatalf("stored body %q, want the trimmed form", msg.Body)
	}

	// A missing recipient id and malformed JSON stay 400s, never 500s.
	if status, _, errMsg := sendDM(t, fx.ts, token, "", "hi"); status != http.StatusBadRequest {
		t.Fatalf("missing to status %d (error %q), want 400", status, errMsg)
	}
	req, err := http.NewRequest(http.MethodPost, fx.ts.URL+"/api/dm", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	if resp, err := fx.ts.Client().Do(req); err != nil {
		t.Fatalf("malformed post: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("malformed body status %d, want 400", resp.StatusCode)
		}
	}
}

// TestSendToUnknownUser: a recipient id matching no account answers 404.
func TestSendToUnknownUser(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "sender")

	status, _, errMsg := sendDM(t, fx.ts, token, "no-such-user", "anyone there?")
	if status != http.StatusNotFound {
		t.Fatalf("unknown recipient status %d, want 404", status)
	}
	if !strings.Contains(errMsg, "not found") {
		t.Fatalf("error %q does not say not found", errMsg)
	}
}

// TestSendToOfflineUser: a real account that never joined presence answers
// 409 — the UI picks recipients from the online list, so an offline recipient
// means the list is stale.
func TestSendToOfflineUser(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "sender")
	recipient, _ := session(t, fx, "ghost") // registered, never online

	status, _, errMsg := sendDM(t, fx.ts, token, recipient.ID, "hello?")
	if status != http.StatusConflict {
		t.Fatalf("offline recipient status %d, want 409", status)
	}
	if !strings.Contains(errMsg, "offline") {
		t.Fatalf("error %q does not explain the offline recipient", errMsg)
	}
}

// TestSendStampsAuthorScopeAndReply checks the 201 reply shape: the server
// stamps the author identity and time, and derives the conversation scope
// dm:<min(idA,idB)>:<max(idA,idB)> regardless of who sends.
func TestSendStampsAuthorScopeAndReply(t *testing.T) {
	fx := newFixture(t, nil)
	bob, bobToken := session(t, fx, "bob")
	carol, _ := session(t, fx, "carol")
	fx.presence.online[carol.ID] = true

	before := time.Now().Unix()
	status, reply, errMsg := sendDM(t, fx.ts, bobToken, carol.ID, "hi carol")
	if status != http.StatusCreated {
		t.Fatalf("send status %d (error %q), want 201", status, errMsg)
	}
	if reply.ID == 0 || reply.AuthorID != bob.ID || reply.AuthorName != "bob" || reply.Body != "hi carol" {
		t.Fatalf("reply %+v does not carry the server-stamped message", reply)
	}
	if reply.To != carol.ID {
		t.Fatalf("reply to %q, want %q", reply.To, carol.ID)
	}
	if reply.Scope != conversationScope(bob.ID, carol.ID) {
		t.Fatalf("reply scope %q, want the canonical pair scope %q", reply.Scope, conversationScope(bob.ID, carol.ID))
	}
	if reply.CreatedAt < before {
		t.Fatalf("reply created_at %d predates the request %d", reply.CreatedAt, before)
	}
}

// TestSendDeliversOnlyToTheTwoDMTopics covers AE4 at the package level: one
// DM reaches the recipient's private topic and the sender's own copy on
// their private topic — and nothing else. Two sentinel publishes prove the
// room topic and a bystander's dm topic stayed silent: anything that leaked
// would have been published before its sentinel and so would be read first.
func TestSendDeliversOnlyToTheTwoDMTopics(t *testing.T) {
	fx := newFixture(t, nil)
	bob, bobToken := session(t, fx, "bob")
	carol, _ := session(t, fx, "carol")
	dave, _ := session(t, fx, "dave")
	fx.presence.online[carol.ID] = true

	carolStream := subscribe(t, fx.ts, store.DMTopic(carol.DMSecret))
	bobStream := subscribe(t, fx.ts, store.DMTopic(bob.DMSecret))
	daveStream := subscribe(t, fx.ts, store.DMTopic(dave.DMSecret))
	roomStream := subscribe(t, fx.ts, "room")
	for _, s := range []*sseReader{carolStream, bobStream, daveStream, roomStream} {
		if event, _ := s.next(t); event != "connected" {
			t.Fatalf("first event %q, want connected", event)
		}
	}

	status, reply, errMsg := sendDM(t, fx.ts, bobToken, carol.ID, "just between us")
	if status != http.StatusCreated {
		t.Fatalf("send status %d (error %q), want 201", status, errMsg)
	}

	if got := carolStream.nextMessage(t); got != reply {
		t.Fatalf("carol's dm stream got %+v, want %+v", got, reply)
	}
	if got := bobStream.nextMessage(t); got != reply {
		t.Fatalf("bob's own dm stream got %+v, want his copy %+v", got, reply)
	}

	// Nothing may arrive on the room topic or dave's topic before the
	// sentinels published strictly after the DM.
	if err := fx.push.PublishJSON("room", "sentinel", map[string]string{"k": "room"}); err != nil {
		t.Fatalf("publish room sentinel: %v", err)
	}
	if event, _ := roomStream.next(t); event != "sentinel" {
		t.Fatalf("room stream saw %q before the sentinel; the DM leaked to the room", event)
	}
	if err := fx.push.PublishJSON(store.DMTopic(dave.DMSecret), "sentinel", map[string]string{"k": "dave"}); err != nil {
		t.Fatalf("publish dave sentinel: %v", err)
	}
	if event, _ := daveStream.next(t); event != "sentinel" {
		t.Fatalf("dave's dm stream saw %q before the sentinel; carol's DM leaked to dave", event)
	}
}

// TestHistoryIsCallerRelativeAndPairScoped pins the participant
// authorization by construction (R8): GET /api/dm?with=<id> always derives
// the scope from the caller and the named user, so dave asking for bob gets
// only dave↔bob messages — never bob↔carol's exchange.
func TestHistoryIsCallerRelativeAndPairScoped(t *testing.T) {
	fx := newFixture(t, nil)
	bob, bobToken := session(t, fx, "bob")
	carol, carolToken := session(t, fx, "carol")
	_, daveToken := session(t, fx, "dave")
	fx.presence.online[bob.ID] = true
	fx.presence.online[carol.ID] = true

	_, first, errMsg := sendDM(t, fx.ts, bobToken, carol.ID, "b1")
	if first.ID == 0 {
		t.Fatalf("bob's send failed: %q", errMsg)
	}
	_, second, errMsg := sendDM(t, fx.ts, carolToken, bob.ID, "c1")
	if second.ID == 0 {
		t.Fatalf("carol's send failed: %q", errMsg)
	}

	// Both participants see the same exchange, oldest-first.
	for name, token := range map[string]string{"bob": bobToken, "carol": carolToken} {
		status, messages := dmHistory(t, fx.ts, token, map[string]string{"bob": carol.ID, "carol": bob.ID}[name])
		if status != http.StatusOK {
			t.Fatalf("%s history status %d, want 200", name, status)
		}
		if len(messages) != 2 || messages[0] != first || messages[1] != second {
			t.Fatalf("%s history %+v, want [%+v %+v] oldest-first", name, messages, first, second)
		}
	}

	// The third authenticated user cannot read bob↔carol's exchange: dave's
	// with=bob query is scoped to dave↔bob, which is empty.
	for _, with := range []string{bob.ID, carol.ID} {
		status, messages := dmHistory(t, fx.ts, daveToken, with)
		if status != http.StatusOK {
			t.Fatalf("dave history status %d, want 200", status)
		}
		if len(messages) != 0 {
			t.Fatalf("dave's with=%s history returned %+v; the third user must not read others' exchanges", with, messages)
		}
	}

	// Unknown with users are 404; a missing with is a 400.
	if status, _ := dmHistory(t, fx.ts, daveToken, "no-such-user"); status != http.StatusNotFound {
		t.Fatalf("unknown with status %d, want 404", status)
	}
	req, err := http.NewRequest(http.MethodGet, fx.ts.URL+"/api/dm", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: daveToken})
	if resp, err := fx.ts.Client().Do(req); err != nil {
		t.Fatalf("missing-with get: %v", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("missing with status %d, want 400", resp.StatusCode)
		}
	}
}

// TestHistoryCappedAtFifty seeds 60 DM rows for one pair and checks the
// history endpoint returns the most recent 50, oldest-first.
func TestHistoryCappedAtFifty(t *testing.T) {
	fx := newFixture(t, nil)
	bob, _ := session(t, fx, "bob")
	carol, carolToken := session(t, fx, "carol")

	ctx := context.Background()
	for i := 1; i <= 60; i++ {
		if _, err := fx.store.AppendMessage(ctx, conversationScope(bob.ID, carol.ID), "u-bob", "bob", "m", int64(1000+i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	status, messages := dmHistory(t, fx.ts, carolToken, bob.ID)
	if status != http.StatusOK {
		t.Fatalf("history status %d, want 200", status)
	}
	if len(messages) != 50 {
		t.Fatalf("history returned %d messages, want the 50 cap", len(messages))
	}
	for i, m := range messages {
		wantID := int64(i + 11) // ids 11..60, oldest first
		if m.ID != wantID {
			t.Fatalf("message %d has id %d, want %d (oldest-first)", i, m.ID, wantID)
		}
	}
}

// TestPublishFailureReturns502AndKeepsRow pins the publish-failure policy
// (mirroring chat): the POST answers 502 while the row stays in the dm
// history.
func TestPublishFailureReturns502AndKeepsRow(t *testing.T) {
	fx := newFixture(t, failingPublisher{err: errors.New("relay is down")})
	_, token := session(t, fx, "unlucky")
	recipient, _ := session(t, fx, "ghosted")
	fx.presence.online[recipient.ID] = true

	status, _, errMsg := sendDM(t, fx.ts, token, recipient.ID, "persisted but not broadcast")
	if status != http.StatusBadGateway {
		t.Fatalf("send status %d, want 502", status)
	}
	if !strings.Contains(errMsg, "not broadcast") {
		t.Fatalf("error %q does not explain the degraded broadcast", errMsg)
	}

	histStatus, messages := dmHistory(t, fx.ts, token, recipient.ID)
	if histStatus != http.StatusOK {
		t.Fatalf("history status %d, want 200", histStatus)
	}
	if len(messages) != 1 || messages[0].Body != "persisted but not broadcast" {
		t.Fatalf("history %+v, want the persisted-but-unpublished row", messages)
	}
}

// TestUserLookup serves the name lookup DM headers need: minimal public info
// for an existing user, 404 for anyone else, and never a dm secret.
func TestUserLookup(t *testing.T) {
	fx := newFixture(t, nil)
	_, token := session(t, fx, "asker")
	carol, _ := session(t, fx, "carol")

	req, err := http.NewRequest(http.MethodGet, fx.ts.URL+"/api/users/"+carol.ID, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	resp, err := fx.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("get /api/users/%s: %v", carol.ID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("user lookup status %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var user struct {
		ID          string `json:"id"`
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
	}
	if err := json.Unmarshal(raw, &user); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if user.ID != carol.ID || user.Username != "carol" || user.DisplayName != "carol" {
		t.Fatalf("user lookup %+v, want carol's public info", user)
	}
	if strings.Contains(string(raw), carol.DMSecret) || strings.Contains(string(raw), "dm_topic") {
		t.Fatalf("user lookup %q leaks the dm secret or topic", raw)
	}

	notFound, err := http.NewRequest(http.MethodGet, fx.ts.URL+"/api/users/no-such-user", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	notFound.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	if resp, err := fx.ts.Client().Do(notFound); err != nil {
		t.Fatalf("unknown user lookup: %v", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown user lookup status %d, want 404", resp.StatusCode)
		}
	}
}
