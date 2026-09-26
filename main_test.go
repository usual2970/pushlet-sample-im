package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/usual2970/pushlet"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/presence"
)

// testPollInterval keeps novaque relay round trips well inside the read
// deadlines used by the stream tests.
const testPollInterval = 25 * time.Millisecond

// newTestApp returns a started App served over an ephemeral port with its
// relay and application databases inside t.TempDir(). The registered
// cleanups close the test server first and stop the app after (LIFO order),
// mirroring production shutdown.
func newTestApp(t *testing.T) (*App, *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	app, err := NewApp(Config{
		DBPath:       filepath.Join(dir, "relay.db"),
		AppDBPath:    filepath.Join(dir, "app.db"),
		PollInterval: testPollInterval,
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	app.Start()
	t.Cleanup(app.Stop)
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)
	return app, ts
}

// TestConfigFromEnv pins the environment surface: the defaults listen on
// :8080 with both SQLite files under data/, and SAMPLE_IM_ADDR /
// SAMPLE_IM_DATA_DIR override their pieces — the data directory placing the
// relay and application databases side by side under one root.
func TestConfigFromEnv(t *testing.T) {
	cfg := configFromEnv()
	if cfg.Addr != defaultAddr {
		t.Fatalf("default addr %q, want %q", cfg.Addr, defaultAddr)
	}
	if want := filepath.Join(defaultDataDir, "relay.db"); cfg.DBPath != want {
		t.Fatalf("default relay db path %q, want %q", cfg.DBPath, want)
	}
	if want := filepath.Join(defaultDataDir, "app.db"); cfg.AppDBPath != want {
		t.Fatalf("default app db path %q, want %q", cfg.AppDBPath, want)
	}

	dir := filepath.Join(t.TempDir(), "state")
	t.Setenv("SAMPLE_IM_ADDR", "127.0.0.1:9999")
	t.Setenv("SAMPLE_IM_DATA_DIR", dir)
	cfg = configFromEnv()
	if cfg.Addr != "127.0.0.1:9999" {
		t.Fatalf("SAMPLE_IM_ADDR ignored: addr %q", cfg.Addr)
	}
	if cfg.DBPath != filepath.Join(dir, "relay.db") {
		t.Fatalf("relay db path %q, want it under %s", cfg.DBPath, dir)
	}
	if cfg.AppDBPath != filepath.Join(dir, "app.db") {
		t.Fatalf("app db path %q, want it under %s", cfg.AppDBPath, dir)
	}
}

func TestHealthRoute(t *testing.T) {
	_, ts := newTestApp(t)

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("get /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /health body: %v", err)
	}
	if !strings.Contains(string(body), "ok") {
		t.Fatalf("body %q, want it to report ok", body)
	}
}

func TestIndexRoute(t *testing.T) {
	_, ts := newTestApp(t)

	// Anonymous visitors are redirected to the login page, never shown an
	// API map: the site root IS the chat page.
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("anonymous status %d, want 302", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/login" {
		t.Fatalf("anonymous Location %q, want /login", loc)
	}

	// Signed in, the root serves the chat shell itself.
	session, _ := registerViaAPI(t, ts, "indexuser", "indexpass")
	authed, err := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	authed.Header.Set("Cookie", auth.SessionCookieName+"="+session)
	page, err := http.DefaultClient.Do(authed)
	if err != nil {
		t.Fatalf("authed get /: %v", err)
	}
	defer page.Body.Close()
	if page.StatusCode != http.StatusOK {
		t.Fatalf("authed status %d, want 200", page.StatusCode)
	}
	body, _ := io.ReadAll(page.Body)
	if !strings.Contains(string(body), `id="messages"`) {
		t.Fatalf("authed / did not render the chat shell")
	}
}

// TestAPIMethodPatternsAndJSONErrors pins the method-pattern mounts: a
// request whose path matches an /api route but whose verb does not is
// answered by the mux's built-in 405 — Allow header intact — instead of
// running the handler, so a cross-site top-level GET can no longer log the
// user out (logout CSRF under SameSite=Lax cookies). Both that 405 and the
// 404 for an unknown /api path render in the JSON envelope every other /api
// answer uses, never the mux's text/plain bodies or the index page.
func TestAPIMethodPatternsAndJSONErrors(t *testing.T) {
	_, ts := newTestApp(t)

	// GET /api/logout: only POST is registered.
	resp, err := http.Get(ts.URL + "/api/logout")
	if err != nil {
		t.Fatalf("get /api/logout: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/logout status %d, want 405", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, http.MethodPost) {
		t.Fatalf("405 Allow header %q, want it to advertise POST", allow)
	}
	assertJSONErrorBody(t, resp)

	// An unknown /api path is a 404 in the same envelope.
	nope, err := http.Get(ts.URL + "/api/nope")
	if err != nil {
		t.Fatalf("get /api/nope: %v", err)
	}
	defer nope.Body.Close()
	if nope.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /api/nope status %d, want 404", nope.StatusCode)
	}
	assertJSONErrorBody(t, nope)
}

// assertJSONErrorBody fails the test unless resp carries the /api error
// envelope: a JSON content type and a non-empty error message.
func assertJSONErrorBody(t *testing.T, resp *http.Response) {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("%s %s: content type %q, want the JSON envelope", resp.Request.Method, resp.Request.URL.Path, ct)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("%s %s: decode error body: %v", resp.Request.Method, resp.Request.URL.Path, err)
	}
	if body.Error == "" {
		t.Fatalf("%s %s: error body carries no message", resp.Request.Method, resp.Request.URL.Path)
	}
}

// TestAuthRoutesMountedThroughApp proves the auth surface (U2) is wired into
// the whole App: registering through the API sets a cookie that carries an
// authenticated request through the middleware to the /chat page.
func TestAuthRoutesMountedThroughApp(t *testing.T) {
	_, ts := newTestApp(t)

	resp, err := http.Post(ts.URL+"/api/register", "application/json",
		strings.NewReader(`{"username":"wired","password":"pw"}`))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status %d, want 200", resp.StatusCode)
	}
	var session string
	for _, c := range resp.Cookies() {
		if c.Name == auth.SessionCookieName {
			session = c.Value
		}
	}
	if session == "" {
		t.Fatal("register through the app set no session cookie")
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/chat", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session})
	chat, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get /chat: %v", err)
	}
	defer chat.Body.Close()
	body, err := io.ReadAll(chat.Body)
	if err != nil {
		t.Fatalf("read /chat body: %v", err)
	}
	if chat.StatusCode != http.StatusOK {
		t.Fatalf("/chat status %d, want 200", chat.StatusCode)
	}
	if !strings.Contains(string(body), "wired") {
		t.Fatalf("/chat body %q does not greet the registered user", body)
	}

	// Anonymous page navigation is redirected to the login page.
	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	anon, err := noRedirect.Get(ts.URL + "/chat")
	if err != nil {
		t.Fatalf("get /chat anonymously: %v", err)
	}
	defer anon.Body.Close()
	if anon.StatusCode != http.StatusFound {
		t.Fatalf("anonymous /chat status %d, want 302", anon.StatusCode)
	}
	if loc := anon.Header.Get("Location"); loc != "/login" {
		t.Fatalf("anonymous /chat redirects to %q, want /login", loc)
	}
}

// TestLoginPageLoginUserAndPresenceLeaveMountedThroughApp covers the routes
// the other end-to-end tests skip, all through main.go's real mount table:
// the /login page (anonymous visitors see both forms, signed-in ones are sent
// straight to /chat), POST /api/login (right credentials 200 plus a fresh
// cookie, wrong ones a 401 in the JSON envelope), GET /api/users/{id} (the
// public name for a known id, 404 for anyone else), the sendBeacon-shaped
// POST /api/presence/leave, and POST /api/logout (the cookie cleared, the old
// session rejected afterwards).
func TestLoginPageLoginUserAndPresenceLeaveMountedThroughApp(t *testing.T) {
	_, ts := newTestApp(t)
	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	// Anonymous /login renders both the sign-in and the registration form.
	anon, err := noRedirect.Get(ts.URL + "/login")
	if err != nil {
		t.Fatalf("get /login anonymously: %v", err)
	}
	defer anon.Body.Close()
	if anon.StatusCode != http.StatusOK {
		t.Fatalf("anonymous /login status %d, want 200", anon.StatusCode)
	}
	page, err := io.ReadAll(anon.Body)
	if err != nil {
		t.Fatalf("read /login page: %v", err)
	}
	for _, want := range []string{`id="login-form"`, `id="register-form"`, "/api/login", "/api/register"} {
		if !strings.Contains(string(page), want) {
			t.Fatalf("anonymous /login page is missing %q", want)
		}
	}

	// A signed-in visitor is sent straight to the room.
	cookie, id := registerViaAPI(t, ts, "mountie", "pw-mountie")
	authedReq, err := http.NewRequest(http.MethodGet, ts.URL+"/login", nil)
	if err != nil {
		t.Fatalf("new /login request: %v", err)
	}
	authedReq.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie})
	authed, err := noRedirect.Do(authedReq)
	if err != nil {
		t.Fatalf("get /login signed in: %v", err)
	}
	defer authed.Body.Close()
	if authed.StatusCode != http.StatusFound {
		t.Fatalf("signed-in /login status %d, want 302", authed.StatusCode)
	}
	if loc := authed.Header.Get("Location"); loc != "/chat" {
		t.Fatalf("signed-in /login redirects to %q, want /chat", loc)
	}

	// POST /api/login with the right credentials signs in and sets a cookie.
	login, err := http.Post(ts.URL+"/api/login", "application/json",
		strings.NewReader(`{"username":"mountie","password":"pw-mountie"}`))
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer login.Body.Close()
	if login.StatusCode != http.StatusOK {
		t.Fatalf("login status %d, want 200", login.StatusCode)
	}
	var loginBody struct {
		ID       string `json:"id"`
		Username string `json:"username"`
	}
	if err := json.NewDecoder(login.Body).Decode(&loginBody); err != nil {
		t.Fatalf("decode login reply: %v", err)
	}
	if loginBody.ID != id || loginBody.Username != "mountie" {
		t.Fatalf("login reply %+v, want the registered account %s", loginBody, id)
	}
	freshCookie := ""
	for _, c := range login.Cookies() {
		if c.Name == auth.SessionCookieName {
			freshCookie = c.Value
		}
	}
	if freshCookie == "" {
		t.Fatal("login set no session cookie")
	}

	// Wrong credentials answer 401 in the JSON envelope.
	wrong, err := http.Post(ts.URL+"/api/login", "application/json",
		strings.NewReader(`{"username":"mountie","password":"not-the-password"}`))
	if err != nil {
		t.Fatalf("login with wrong password: %v", err)
	}
	defer wrong.Body.Close()
	if wrong.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-password login status %d, want 401", wrong.StatusCode)
	}
	assertJSONErrorBody(t, wrong)

	// GET /api/users/{id} carries the public name; unknown ids answer 404.
	userReq, err := http.NewRequest(http.MethodGet, ts.URL+"/api/users/"+id, nil)
	if err != nil {
		t.Fatalf("new user lookup: %v", err)
	}
	userReq.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie})
	userResp, err := http.DefaultClient.Do(userReq)
	if err != nil {
		t.Fatalf("get /api/users/%s: %v", id, err)
	}
	defer userResp.Body.Close()
	if userResp.StatusCode != http.StatusOK {
		t.Fatalf("user lookup status %d, want 200", userResp.StatusCode)
	}
	var user struct {
		ID          string `json:"id"`
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
	}
	if err := json.NewDecoder(userResp.Body).Decode(&user); err != nil {
		t.Fatalf("decode user lookup: %v", err)
	}
	if user.ID != id || user.Username != "mountie" || user.DisplayName != "mountie" {
		t.Fatalf("user lookup %+v, want mountie's public info under id %s", user, id)
	}

	nopeReq, err := http.NewRequest(http.MethodGet, ts.URL+"/api/users/no-such-user", nil)
	if err != nil {
		t.Fatalf("new unknown user lookup: %v", err)
	}
	nopeReq.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie})
	nope, err := http.DefaultClient.Do(nopeReq)
	if err != nil {
		t.Fatalf("get /api/users/no-such-user: %v", err)
	}
	defer nope.Body.Close()
	if nope.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown user lookup status %d, want 404", nope.StatusCode)
	}
	assertJSONErrorBody(t, nope)

	// The sendBeacon-shaped leave (empty body, no Content-Type) answers 200.
	joinPresence(t, ts, cookie)
	leave, err := http.NewRequest(http.MethodPost, ts.URL+"/api/presence/leave", nil)
	if err != nil {
		t.Fatalf("new leave: %v", err)
	}
	leave.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie})
	leaveResp, err := http.DefaultClient.Do(leave)
	if err != nil {
		t.Fatalf("post /api/presence/leave: %v", err)
	}
	leaveResp.Body.Close()
	if leaveResp.StatusCode != http.StatusOK {
		t.Fatalf("presence leave status %d, want 200", leaveResp.StatusCode)
	}

	// Logout clears the cookie, and the old session stops working.
	logout, err := http.NewRequest(http.MethodPost, ts.URL+"/api/logout", nil)
	if err != nil {
		t.Fatalf("new logout: %v", err)
	}
	logout.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie})
	logoutResp, err := http.DefaultClient.Do(logout)
	if err != nil {
		t.Fatalf("post /api/logout: %v", err)
	}
	defer logoutResp.Body.Close()
	if logoutResp.StatusCode != http.StatusOK {
		t.Fatalf("logout status %d, want 200", logoutResp.StatusCode)
	}
	cleared := false
	for _, c := range logoutResp.Cookies() {
		if c.Name == auth.SessionCookieName && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("logout did not clear the session cookie")
	}

	meReq, err := http.NewRequest(http.MethodGet, ts.URL+"/api/me", nil)
	if err != nil {
		t.Fatalf("new /api/me request: %v", err)
	}
	meReq.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie})
	me, err := http.DefaultClient.Do(meReq)
	if err != nil {
		t.Fatalf("get /api/me with the logged-out cookie: %v", err)
	}
	defer me.Body.Close()
	if me.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/api/me with the logged-out cookie: status %d, want 401", me.StatusCode)
	}
}

func TestSSEStreamSendsConnectedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, ts := newTestApp(t)

	stream := openSSE(t, ctx, ts.URL+"/events?topic=room")
	event, data := stream.next(t)
	if event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}
	if data == "" {
		t.Fatal("connected event carried empty data")
	}
}

// TestPublishJSONReachesSSESubscriberThroughRelay proves the novaque relay
// path end to end on SQLite: PublishJSON writes to the relay topic, the
// consumer polls it back, and the broker delivers to the subscribed client.
func TestPublishJSONReachesSSESubscriberThroughRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	app, ts := newTestApp(t)

	stream := openSSE(t, ctx, ts.URL+"/events?topic=room")
	if event, _ := stream.next(t); event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}

	sent := map[string]string{"from": "alice", "text": "hello room"}
	if err := app.push.PublishJSON("room", "chat", sent); err != nil {
		t.Fatalf("publish: %v", err)
	}

	for {
		event, data := stream.next(t)
		if event != "chat" {
			t.Logf("skipping event %q while waiting for chat", event)
			continue
		}
		var got map[string]string
		if err := json.Unmarshal([]byte(data), &got); err != nil {
			t.Fatalf("decode chat data %q: %v", data, err)
		}
		if got["from"] != sent["from"] || got["text"] != sent["text"] {
			t.Fatalf("got %v, want %v", got, sent)
		}
		return
	}
}

func TestWebSocketSubscriberReceivesPublishedMessage(t *testing.T) {
	app, ts := newTestApp(t)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?topic=room"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", wsURL, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	readMessage := func() pushlet.Message {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read websocket message: %v", err)
		}
		return parseWebSocketFrame(t, raw)
	}

	hello := readMessage()
	if hello.Event != "connected" {
		t.Fatalf("first websocket event %q, want connected", hello.Event)
	}

	sent := map[string]string{"from": "bob", "text": "hello ws"}
	if err := app.push.PublishJSON("room", "chat", sent); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg := readMessage()
	if msg.Event != "chat" {
		t.Fatalf("event %q, want chat", msg.Event)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(msg.Data), &got); err != nil {
		t.Fatalf("decode chat data %q: %v", msg.Data, err)
	}
	if got["text"] != sent["text"] {
		t.Fatalf("got %v, want text %q", got, sent["text"])
	}
}

// TestChatRoomEndToEndThroughRelay drives the full U3 chain across every
// layer with no mocks: register over HTTP, subscribe an SSE client to the
// room topic, post a multi-line unicode message over the API, watch it fan
// out through the novaque relay to the subscriber, and read it back from
// history and the rendered room page.
func TestChatRoomEndToEndThroughRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, ts := newTestApp(t)

	resp, err := http.Post(ts.URL+"/api/register", "application/json",
		strings.NewReader(`{"username":"roomie","password":"pw"}`))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status %d, want 200", resp.StatusCode)
	}
	var session string
	for _, c := range resp.Cookies() {
		if c.Name == auth.SessionCookieName {
			session = c.Value
		}
	}
	if session == "" {
		t.Fatal("register set no session cookie")
	}

	stream := openSSE(t, ctx, ts.URL+"/events?topic=room")
	if event, _ := stream.next(t); event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}

	body := "hello room\nsecond line 你好 🎉"
	payload, err := json.Marshal(map[string]string{"body": body})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	post, err := http.NewRequest(http.MethodPost, ts.URL+"/api/messages", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	post.Header.Set("Content-Type", "application/json")
	post.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session})
	postResp, err := http.DefaultClient.Do(post)
	if err != nil {
		t.Fatalf("post message: %v", err)
	}
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusCreated {
		t.Fatalf("post message status %d, want 201", postResp.StatusCode)
	}
	var reply struct {
		ID         int64  `json:"id"`
		AuthorName string `json:"author_name"`
		Body       string `json:"body"`
		CreatedAt  int64  `json:"created_at"`
	}
	if err := json.NewDecoder(postResp.Body).Decode(&reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.Body != body || reply.AuthorName != "roomie" || reply.ID == 0 || reply.CreatedAt == 0 {
		t.Fatalf("reply %+v is not the server-stamped message", reply)
	}

	for {
		event, data := stream.next(t)
		if event != "message" {
			t.Logf("skipping event %q while waiting for message", event)
			continue
		}
		var got struct {
			ID         int64  `json:"id"`
			AuthorName string `json:"author_name"`
			Body       string `json:"body"`
			CreatedAt  int64  `json:"created_at"`
		}
		if err := json.Unmarshal([]byte(data), &got); err != nil {
			t.Fatalf("decode message data %q: %v", data, err)
		}
		if got != reply {
			t.Fatalf("stream payload %+v, want the posted reply %+v", got, reply)
		}
		break
	}

	// History reads the same row back from SQLite.
	hist, err := http.NewRequest(http.MethodGet, ts.URL+"/api/messages?after=0", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	hist.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session})
	histResp, err := http.DefaultClient.Do(hist)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	defer histResp.Body.Close()
	var messages []struct {
		ID       int64  `json:"id"`
		Body     string `json:"body"`
		AuthorID string `json:"author_id"`
	}
	if err := json.NewDecoder(histResp.Body).Decode(&messages); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != reply.ID || messages[0].Body != body || messages[0].AuthorID == "" {
		t.Fatalf("history %+v, want the posted message %+v", messages, reply)
	}
}

// TestStaticAssetsServed checks the embedded client assets are reachable
// under /static/ through the app's mux.
func TestStaticAssetsServed(t *testing.T) {
	_, ts := newTestApp(t)

	for path, want := range map[string]string{
		"/static/app.js":    "EventSource", // the room stream subscription
		"/static/style.css": "conn-banner", // the reconnect banner style
	} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("get %s status %d, want 200", path, resp.StatusCode)
		}
		if !strings.Contains(string(body), want) {
			t.Fatalf("%s does not contain %q", path, want)
		}
	}

	// The client script wires both streams: the fixed room topic and the
	// caller's private dm topic resolved from /api/me.
	resp, err := http.Get(ts.URL + "/static/app.js")
	if err != nil {
		t.Fatalf("get /static/app.js: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read /static/app.js: %v", err)
	}
	for _, want := range []string{"/api/me", "/api/dm"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("app.js does not reference %q", want)
		}
	}
}

// TestPushHandlersUnavailableBeforeStart pins the ordering constraint:
// handlers mounted by NewApp answer 503 until App.Start runs the broker, so
// Start must be called before serving traffic.
func TestPushHandlersUnavailableBeforeStart(t *testing.T) {
	dir := t.TempDir()
	app, err := NewApp(Config{
		DBPath:       filepath.Join(dir, "relay.db"),
		AppDBPath:    filepath.Join(dir, "app.db"),
		PollInterval: testPollInterval,
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(app.Stop)
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events?topic=room", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get /events before start: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
}

func TestRedactingLoggerRedactsDMTopics(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	newRedactingLogger().
		WithField("client_id", "c1").
		WithField("topic", "dm:alice/bob").
		Println("New client requested connection")

	out := buf.String()
	if strings.Contains(out, "alice") {
		t.Fatalf("dm topic leaked to log: %q", out)
	}
	if !strings.Contains(out, redactedTopic) {
		t.Fatalf("log %q missing redaction marker %q", out, redactedTopic)
	}
	if !strings.Contains(out, "New client requested connection") {
		t.Fatalf("log %q dropped the message", out)
	}

	// Non-dm topics pass through, and WithField leaves the parent untouched.
	buf.Reset()
	base := newRedactingLogger().WithField("topic", "room")
	base.WithField("client_id", "c2").Println("scoped")
	if !strings.Contains(buf.String(), "room") || !strings.Contains(buf.String(), "c2") {
		t.Fatalf("log %q lost pass-through fields", buf.String())
	}
	buf.Reset()
	base.Println("base")
	if strings.Contains(buf.String(), "c2") {
		t.Fatalf("WithField mutated the parent logger: %q", buf.String())
	}
}

// TestRedactingLoggerScrubsRawOperands pins the operand half of the redaction
// contract: pushlet's WebSocket read loop logs received command lines as
// Println operands ("Received command:", "SUB dm:<secret>") rather than
// fields, so those must be scrubbed too — everything from the dm: prefix
// onward disappears, in both the string and []byte shapes that path logs.
// The WithField("topic", "dm:…") redaction keeps working alongside.
func TestRedactingLoggerScrubsRawOperands(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	newRedactingLogger().Println("Received command:", "SUB dm:supersecret")
	out := buf.String()
	if strings.Contains(out, "supersecret") {
		t.Fatalf("raw operand leaked the dm secret: %q", out)
	}
	if !strings.Contains(out, "SUB "+redactedTopic) {
		t.Fatalf("log %q missing the redacted command %q", out, "SUB "+redactedTopic)
	}
	if !strings.Contains(out, "Received command:") {
		t.Fatalf("log %q dropped the surrounding operands", out)
	}

	// The WebSocket loop logs the command line as []byte; same scrub.
	buf.Reset()
	newRedactingLogger().Println("Received command:", []byte("SUB dm:supersecret"))
	if out = buf.String(); strings.Contains(out, "supersecret") {
		t.Fatalf("byte-slice operand leaked the dm secret: %q", out)
	}

	// Field and operand redaction coexist on one log line.
	buf.Reset()
	newRedactingLogger().
		WithField("topic", "dm:another-secret").
		Println("New client requested connection", "SUB dm:supersecret")
	out = buf.String()
	if strings.Contains(out, "another-secret") || strings.Contains(out, "supersecret") {
		t.Fatalf("log %q leaked a dm topic through a field or an operand", out)
	}
	if !strings.Contains(out, redactedTopic) {
		t.Fatalf("log %q missing the redaction marker %q", out, redactedTopic)
	}

	// Operands without a dm: topic pass through untouched.
	buf.Reset()
	newRedactingLogger().Println("Received command:", "SUB room")
	if out = buf.String(); !strings.Contains(out, "SUB room") {
		t.Fatalf("log %q mangled a plain topic operand", out)
	}
}

// TestPresenceRoutesMountedThroughApp proves the presence surface (U4) is
// wired into the whole App, including the duplicate-name disambiguation
// (KTD5): two accounts sharing the username "twin" join, both render as
// twin#<id suffix>, and after one leaves through a beacon-shaped POST the
// survivor reverts to the plain name.
func TestPresenceRoutesMountedThroughApp(t *testing.T) {
	_, ts := newTestApp(t)

	// Anonymous presence calls are rejected like every other /api route.
	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	for _, path := range []string{"/api/presence/join", "/api/presence/heartbeat", "/api/presence/leave"} {
		resp, err := noRedirect.Post(ts.URL+path, "", nil)
		if err != nil {
			t.Fatalf("anonymous post %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("anonymous POST %s status %d, want 401", path, resp.StatusCode)
		}
	}

	// Two accounts may share a name when their passwords differ.
	cookie1, id1 := registerViaAPI(t, ts, "twin", "one")
	cookie2, id2 := registerViaAPI(t, ts, "twin", "two")
	if id1 == id2 {
		t.Fatal("the two twin accounts share one id")
	}

	first := joinPresence(t, ts, cookie1)
	if u, ok := findPresenceUser(first, id1); !ok || u.Display != "twin" {
		t.Fatalf("lone twin entry %+v (found %v), want the plain name", u, ok)
	}

	second := joinPresence(t, ts, cookie2)
	u1, ok := findPresenceUser(second, id1)
	if !ok {
		t.Fatalf("second join snapshot %+v lost the first twin", second.Online)
	}
	u2, ok := findPresenceUser(second, id2)
	if !ok {
		t.Fatalf("second join snapshot %+v lost the second twin", second.Online)
	}
	if want := "twin#" + idSuffix(id1); u1.Display != want {
		t.Fatalf("first twin displays %q, want %q", u1.Display, want)
	}
	if want := "twin#" + idSuffix(id2); u2.Display != want {
		t.Fatalf("second twin displays %q, want %q", u2.Display, want)
	}

	// Heartbeat answers 200 and leave accepts a beacon-shaped POST (empty
	// body, no Content-Type) — the exact shape page unload sends.
	hb, err := http.NewRequest(http.MethodPost, ts.URL+"/api/presence/heartbeat", nil)
	if err != nil {
		t.Fatalf("new heartbeat: %v", err)
	}
	hb.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie1})
	hbResp, err := http.DefaultClient.Do(hb)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	hbResp.Body.Close()
	if hbResp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat status %d, want 200", hbResp.StatusCode)
	}

	leave, err := http.NewRequest(http.MethodPost, ts.URL+"/api/presence/leave", nil)
	if err != nil {
		t.Fatalf("new leave: %v", err)
	}
	leave.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie2})
	leaveResp, err := http.DefaultClient.Do(leave)
	if err != nil {
		t.Fatalf("leave: %v", err)
	}
	leaveResp.Body.Close()
	if leaveResp.StatusCode != http.StatusOK {
		t.Fatalf("leave status %d, want 200", leaveResp.StatusCode)
	}

	healed := joinPresence(t, ts, cookie1)
	if got := len(healed.Online); got != 1 {
		t.Fatalf("after the leave %d online (%+v), want the survivor alone", got, healed.Online)
	}
	if u, ok := findPresenceUser(healed, id1); !ok || u.Display != "twin" {
		t.Fatalf("survivor entry %+v (found %v), want the plain name back", u, ok)
	}
}

// TestPresenceJoinReachesSSESubscriberThroughRelay drives presence over the
// novaque relay: a subscribed SSE client sees the join snapshot as a
// "presence" event on the room topic, and the leave snapshot after the
// beacon-shaped POST.
func TestPresenceJoinReachesSSESubscriberThroughRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, ts := newTestApp(t)

	stream := openSSE(t, ctx, ts.URL+"/events?topic=room")
	if event, _ := stream.next(t); event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}

	cookie, id := registerViaAPI(t, ts, "watcher", "pw")
	joinPresence(t, ts, cookie)

	var joined presence.Snapshot
	for {
		event, data := stream.next(t)
		if event != "presence" {
			t.Logf("skipping event %q while waiting for presence", event)
			continue
		}
		if err := json.Unmarshal([]byte(data), &joined); err != nil {
			t.Fatalf("decode presence data %q: %v", data, err)
		}
		break
	}
	if u, ok := findPresenceUser(joined, id); !ok || u.Name != "watcher" || u.Display != "watcher" {
		t.Fatalf("presence event entry %+v (found %v), want watcher", u, ok)
	}

	leave, err := http.NewRequest(http.MethodPost, ts.URL+"/api/presence/leave", nil)
	if err != nil {
		t.Fatalf("new leave: %v", err)
	}
	leave.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: cookie})
	leaveResp, err := http.DefaultClient.Do(leave)
	if err != nil {
		t.Fatalf("leave: %v", err)
	}
	leaveResp.Body.Close()
	if leaveResp.StatusCode != http.StatusOK {
		t.Fatalf("leave status %d, want 200", leaveResp.StatusCode)
	}

	for {
		event, data := stream.next(t)
		if event != "presence" {
			t.Logf("skipping event %q while waiting for presence", event)
			continue
		}
		var snap presence.Snapshot
		if err := json.Unmarshal([]byte(data), &snap); err != nil {
			t.Fatalf("decode presence data %q: %v", data, err)
		}
		if _, ok := findPresenceUser(snap, id); ok {
			continue // a snapshot from before the leave landed
		}
		if got := len(snap.Online); got != 0 {
			t.Fatalf("post-leave snapshot %+v, want it empty", snap.Online)
		}
		return
	}
}

// registerViaAPI creates an account through the HTTP register endpoint and
// returns its session cookie and user id.
func registerViaAPI(t *testing.T, ts *httptest.Server, username, password string) (session, userID string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/api/register", "application/json",
		strings.NewReader(fmt.Sprintf(`{"username":%q,"password":%q}`, username, password)))
	if err != nil {
		t.Fatalf("register %s: %v", username, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register %s status %d, want 200", username, resp.StatusCode)
	}
	for _, c := range resp.Cookies() {
		if c.Name == auth.SessionCookieName {
			session = c.Value
		}
	}
	if session == "" {
		t.Fatalf("register %s set no session cookie", username)
	}
	var reply struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		t.Fatalf("decode register reply: %v", err)
	}
	if reply.ID == "" {
		t.Fatalf("register %s reply carries no id", username)
	}
	return session, reply.ID
}

// joinPresence POSTs the join endpoint carrying the session cookie and
// decodes the online-list snapshot reply.
func joinPresence(t *testing.T, ts *httptest.Server, session string) presence.Snapshot {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/presence/join", nil)
	if err != nil {
		t.Fatalf("new join: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("join status %d, want 200", resp.StatusCode)
	}
	var snap presence.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode join reply: %v", err)
	}
	return snap
}

// findPresenceUser returns the snapshot entry with the given id.
func findPresenceUser(s presence.Snapshot, id string) (presence.User, bool) {
	for _, u := range s.Online {
		if u.ID == id {
			return u, true
		}
	}
	return presence.User{}, false
}

// idSuffix mirrors the display-name suffix the server derives from a user
// id: the last four characters.
func idSuffix(id string) string {
	if len(id) <= 4 {
		return id
	}
	return id[len(id)-4:]
}

// sseStream reads one Server-Sent Events connection into parsed events.
// All reads are bound by the context passed to openSSE.
type sseStream struct {
	lines *bufio.Scanner
}

// openSSE subscribes to urlStr and fails the test unless the endpoint
// answers 200 with a text/event-stream body.
func openSSE(t *testing.T, ctx context.Context, urlStr string) *sseStream {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", urlStr, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get %s: status %d, want 200", urlStr, resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("get %s: content type %q, want text/event-stream", urlStr, ct)
	}
	return &sseStream{lines: bufio.NewScanner(resp.Body)}
}

// next returns the next event/data pair on the stream, skipping heartbeat
// comments. It fails the test when the stream ends or errors before a full
// event arrives.
func (s *sseStream) next(t *testing.T) (event, data string) {
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

// parseWebSocketFrame decodes one pushlet WebSocket frame: the connected
// welcome is bare JSON, published frames are "<topic> <json>".
func parseWebSocketFrame(t *testing.T, raw []byte) pushlet.Message {
	t.Helper()
	payload := string(raw)
	if !strings.HasPrefix(payload, "{") {
		if i := strings.IndexByte(payload, ' '); i >= 0 {
			payload = payload[i+1:]
		}
	}
	var msg pushlet.Message
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		t.Fatalf("decode websocket frame %q: %v", raw, err)
	}
	return msg
}
