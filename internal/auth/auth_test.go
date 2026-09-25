package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/usual2970/sample-im/internal/store"
)

// loginTemplatePath reaches the real page template from this package's
// directory. Production embeds the same file via //go:embed in the main
// package; tests parse it from disk so there is exactly one copy to keep
// honest.
const loginTemplatePath = "../../web/templates/login.html"

// newTestService wires a Service over a fresh store, mounts the same routes
// main.go mounts, and serves them over an ephemeral port.
func newTestService(t *testing.T) (*authFixture, *httptest.Server) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tpl, err := template.ParseFiles(loginTemplatePath)
	if err != nil {
		t.Fatalf("parse login template: %v", err)
	}
	fx := newFixture(t, st, tpl)
	ts := httptest.NewServer(fx.mux)
	t.Cleanup(ts.Close)
	return fx, ts
}

// authFixture groups a service with the mux serving it, so tests can rebuild
// the stack over a reopened store.
type authFixture struct {
	store *store.Store
	svc   *Service
	mux   *http.ServeMux
}

// newFixture mounts the auth routes exactly the way main.go does.
func newFixture(t *testing.T, st *store.Store, tpl *template.Template) *authFixture {
	t.Helper()
	svc := NewService(st, tpl)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/register", svc.HandleRegister)
	mux.HandleFunc("/api/login", svc.HandleLogin)
	mux.Handle("/api/logout", svc.RequireAuth(http.HandlerFunc(svc.HandleLogout)))
	mux.Handle("/api/me", svc.RequireAuth(http.HandlerFunc(svc.HandleMe)))
	mux.HandleFunc("/login", svc.HandleLoginPage)
	mux.Handle("/chat", svc.RequireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _ := FromContext(r.Context())
		fmt.Fprintf(w, "chat coming in U3, %s", user.Username)
	})))
	return &authFixture{store: st, svc: svc, mux: mux}
}

// apiResponse captures one JSON API answer: status, decoded body (when JSON),
// and the session cookie value the response set, if any.
type apiResponse struct {
	status  int
	body    map[string]string
	session string
}

// postCredentials sends username/password as JSON to path and parses the
// response, failing the test on transport errors.
func postCredentials(t *testing.T, client *http.Client, urlStr, path, username, password string) apiResponse {
	t.Helper()
	resp, err := postCredentialsHTTP(client, urlStr, path, username, password)
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	return resp
}

// postCredentialsHTTP is the goroutine-safe core of postCredentials: it
// never touches the testing.T, returning a zero response on error instead.
func postCredentialsHTTP(client *http.Client, urlStr, path, username, password string) (apiResponse, error) {
	payload, err := json.Marshal(map[string]string{"username": username, "password": password})
	if err != nil {
		return apiResponse{}, err
	}
	resp, err := client.Post(urlStr+path, "application/json", strings.NewReader(string(payload)))
	if err != nil {
		return apiResponse{}, err
	}
	defer resp.Body.Close()

	out := apiResponse{status: resp.StatusCode}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
		out.body = body
	}
	for _, c := range resp.Cookies() {
		if c.Name == SessionCookieName {
			out.session = c.Value
		}
	}
	return out, nil
}

// noRedirectClient never follows redirects, so assertions see the raw
// redirect response.
func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// do runs one request through a client, failing the test on transport
// errors, and arranges the body to be closed.
func do(t *testing.T, client *http.Client, r *http.Request) *http.Response {
	t.Helper()
	resp, err := client.Do(r)
	if err != nil {
		t.Fatalf("%s %s: %v", r.Method, r.URL.Path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// getWithCookie fetches urlStr carrying the session cookie (empty token =
// no cookie), without following redirects.
func getWithCookie(t *testing.T, urlStr, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: token})
	}
	return do(t, noRedirectClient(), req)
}

// TestRegisterLoginMeFlow walks the happy path end to end: register sets the
// session cookie, /api/me resolves it, the authed /chat page admits it, and
// login returns the same account.
func TestRegisterLoginMeFlow(t *testing.T) {
	_, ts := newTestService(t)
	client := &http.Client{}

	reg := postCredentials(t, client, ts.URL, "/api/register", "alice", "password-one")
	if reg.status != http.StatusOK {
		t.Fatalf("register status %d, want 200", reg.status)
	}
	if reg.body["username"] != "alice" || reg.body["id"] == "" {
		t.Fatalf("register body %v, want id and username", reg.body)
	}
	if reg.session == "" {
		t.Fatal("register did not set the session cookie")
	}

	me := getWithCookie(t, ts.URL+"/api/me", reg.session)
	if me.StatusCode != http.StatusOK {
		t.Fatalf("/api/me status %d, want 200", me.StatusCode)
	}
	var meBody map[string]string
	if err := json.NewDecoder(me.Body).Decode(&meBody); err != nil {
		t.Fatalf("decode /api/me: %v", err)
	}
	if meBody["id"] != reg.body["id"] || meBody["username"] != "alice" {
		t.Fatalf("/api/me body %v, want the registered account", meBody)
	}

	chat := getWithCookie(t, ts.URL+"/chat", reg.session)
	if chat.StatusCode != http.StatusOK {
		t.Fatalf("/chat status %d, want 200", chat.StatusCode)
	}
	page, _ := io.ReadAll(chat.Body)
	if !strings.Contains(string(page), "chat coming in U3, alice") {
		t.Fatalf("/chat page %q does not greet the signed-in user", page)
	}

	login := postCredentials(t, client, ts.URL, "/api/login", "alice", "password-one")
	if login.status != http.StatusOK {
		t.Fatalf("login status %d, want 200", login.status)
	}
	if login.body["id"] != reg.body["id"] {
		t.Fatalf("login resolved id %s, want %s", login.body["id"], reg.body["id"])
	}
	if login.session == "" {
		t.Fatal("login did not set the session cookie")
	}
}

// TestMeExposesDMTopic pins the KTD6 contract: /api/me is the only endpoint
// that reveals a user's direct-message topic, it derives from the account's
// dm secret (itself at least 22 random chars), and the register/login replies
// never carry it.
func TestMeExposesDMTopic(t *testing.T) {
	fx, ts := newTestService(t)
	client := &http.Client{}

	reg := postCredentials(t, client, ts.URL, "/api/register", "kim", "pw")
	if reg.status != http.StatusOK {
		t.Fatalf("register status %d, want 200", reg.status)
	}
	if _, leaked := reg.body["dm_topic"]; leaked {
		t.Fatalf("register reply %v leaks the dm topic", reg.body)
	}

	meResp := getWithCookie(t, ts.URL+"/api/me", reg.session)
	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/me status %d, want 200", meResp.StatusCode)
	}
	var meBody map[string]string
	if err := json.NewDecoder(meResp.Body).Decode(&meBody); err != nil {
		t.Fatalf("decode /api/me: %v", err)
	}
	if meBody["id"] != reg.body["id"] || meBody["username"] != "kim" {
		t.Fatalf("/api/me body %v lost id/username", meBody)
	}
	topic := meBody["dm_topic"]
	if !strings.HasPrefix(topic, "dm:") {
		t.Fatalf("dm_topic %q does not start with dm:", topic)
	}
	if len(topic) < len("dm:")+22 {
		t.Fatalf("dm_topic %q is shorter than the 22-char secret minimum", topic)
	}

	user, err := fx.store.UserByID(context.Background(), reg.body["id"])
	if err != nil || user == nil {
		t.Fatalf("lookup registered account: %v %v", user, err)
	}
	if len(user.DMSecret) < 22 {
		t.Fatalf("stored dm_secret %q is shorter than 22 chars", user.DMSecret)
	}
	if want := store.DMTopic(user.DMSecret); topic != want {
		t.Fatalf("dm_topic %q, want the topic derived from the account %q", topic, want)
	}
}

// TestRegisterSetsCookieAttributes pins the cookie hardening flags.
func TestRegisterSetsCookieAttributes(t *testing.T) {
	_, ts := newTestService(t)

	raw, err := http.Post(ts.URL+"/api/register", "application/json",
		strings.NewReader(`{"username":"alice","password":"pw"}`))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer raw.Body.Close()
	if raw.StatusCode != http.StatusOK {
		t.Fatalf("register status %d, want 200", raw.StatusCode)
	}

	var cookie *http.Cookie
	for _, c := range raw.Cookies() {
		if c.Name == SessionCookieName {
			cookie = c
		}
	}
	if cookie == nil {
		t.Fatalf("no %s cookie in response", SessionCookieName)
	}
	if !cookie.HttpOnly {
		t.Error("session cookie is not HttpOnly")
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Errorf("session cookie SameSite %v, want Lax", cookie.SameSite)
	}
	if cookie.Path != "/" {
		t.Errorf("session cookie Path %q, want /", cookie.Path)
	}
}

// TestSameNameDifferentPasswordsAreDistinctAccounts covers AE1 through the
// API: both registrations succeed, and each (name, password) login resolves
// to its own account.
func TestSameNameDifferentPasswordsAreDistinctAccounts(t *testing.T) {
	_, ts := newTestService(t)
	client := &http.Client{}

	first := postCredentials(t, client, ts.URL, "/api/register", "bob", "hunter2")
	second := postCredentials(t, client, ts.URL, "/api/register", "bob", "hunter3")
	if first.status != http.StatusOK || second.status != http.StatusOK {
		t.Fatalf("registrations: %d and %d, want 200 and 200", first.status, second.status)
	}
	if first.body["id"] == second.body["id"] {
		t.Fatal("same-name accounts share a user id")
	}

	for pw, wantID := range map[string]string{"hunter2": first.body["id"], "hunter3": second.body["id"]} {
		login := postCredentials(t, client, ts.URL, "/api/login", "bob", pw)
		if login.status != http.StatusOK {
			t.Fatalf("login with %s: status %d, want 200", pw, login.status)
		}
		if login.body["id"] != wantID {
			t.Fatalf("login with %s resolved id %s, want %s", pw, login.body["id"], wantID)
		}
	}
}

// TestRegisterPairCollisionRejected covers AE2: re-registering the same
// (username, password) pair is rejected with a 4xx and a clear error, while
// the same name with a different password succeeds.
func TestRegisterPairCollisionRejected(t *testing.T) {
	_, ts := newTestService(t)
	client := &http.Client{}

	if reg := postCredentials(t, client, ts.URL, "/api/register", "carol", "pw-one"); reg.status != http.StatusOK {
		t.Fatalf("first register status %d, want 200", reg.status)
	}

	dup := postCredentials(t, client, ts.URL, "/api/register", "carol", "pw-one")
	if dup.status < 400 || dup.status >= 500 {
		t.Fatalf("duplicate-pair register status %d, want 4xx", dup.status)
	}
	if !strings.Contains(dup.body["error"], "already exists") {
		t.Fatalf("error %q does not explain the pair collision", dup.body["error"])
	}

	other := postCredentials(t, client, ts.URL, "/api/register", "carol", "pw-two")
	if other.status != http.StatusOK {
		t.Fatalf("same name, different password: status %d, want 200", other.status)
	}
}

// TestRegisterValidation rejects usernames and passwords outside the R1/R2
// rules with 400 and a clear message.
func TestRegisterValidation(t *testing.T) {
	_, ts := newTestService(t)
	client := &http.Client{}

	cases := []struct {
		name     string
		username string
		password string
		wantErr  string
	}{
		{"empty username", "", "pw", "username is required"},
		{"whitespace username", "   ", "pw", "username is required"},
		{"overlong username", strings.Repeat("a", 33), "pw", "at most 32"},
		{"illegal charset", "not ok!", "pw", "letters, digits"},
		{"non-ascii username", "用户", "pw", "letters, digits"},
		{"empty password", "dave", "", "password is required"},
		{"overlong password", "dave", strings.Repeat("x", 73), "at most 72"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postCredentials(t, client, ts.URL, "/api/register", tc.username, tc.password)
			if resp.status != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", resp.status)
			}
			if !strings.Contains(resp.body["error"], tc.wantErr) {
				t.Fatalf("error %q does not contain %q", resp.body["error"], tc.wantErr)
			}
		})
	}

	// The trimmed form of a padded username registers and logs in.
	resp := postCredentials(t, client, ts.URL, "/api/register", "  padded  ", "pw")
	if resp.status != http.StatusOK {
		t.Fatalf("padded username register status %d, want 200", resp.status)
	}
	if resp.body["username"] != "padded" {
		t.Fatalf("registered username %q, want padded", resp.body["username"])
	}
	if login := postCredentials(t, client, ts.URL, "/api/login", "padded", "pw"); login.status != http.StatusOK {
		t.Fatalf("login as trimmed username status %d, want 200", login.status)
	}
}

// TestLoginFailures answers 401 with a message that does not leak which half
// of the credentials was wrong.
func TestLoginFailures(t *testing.T) {
	_, ts := newTestService(t)
	client := &http.Client{}

	if reg := postCredentials(t, client, ts.URL, "/api/register", "erin", "right-password"); reg.status != http.StatusOK {
		t.Fatalf("register status %d, want 200", reg.status)
	}

	for _, tc := range []struct{ username, password string }{
		{"erin", "wrong-password"},
		{"nobody", "right-password"},
	} {
		resp := postCredentials(t, client, ts.URL, "/api/login", tc.username, tc.password)
		if resp.status != http.StatusUnauthorized {
			t.Fatalf("login %s/%s: status %d, want 401", tc.username, tc.password, resp.status)
		}
		if resp.body["error"] != ErrWrongCredentials.Error() {
			t.Fatalf("error %q, want %q", resp.body["error"], ErrWrongCredentials.Error())
		}
	}
}

// TestMiddlewareRejectsAnonymousRequests pins the two rejection shapes: API
// paths get a 401 JSON body, page navigations get redirected to /login. Both
// a missing and a garbage token behave the same.
func TestMiddlewareRejectsAnonymousRequests(t *testing.T) {
	_, ts := newTestService(t)

	for _, token := range []string{"", "garbage-token", "not-a-real-session"} {
		resp := getWithCookie(t, ts.URL+"/api/me", token)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("token %q: /api/me status %d, want 401", token, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
			t.Fatalf("token %q: /api/me content type %q, want JSON", token, ct)
		}

		page := getWithCookie(t, ts.URL+"/chat", token)
		if page.StatusCode < 300 || page.StatusCode >= 400 {
			t.Fatalf("token %q: /chat status %d, want a redirect", token, page.StatusCode)
		}
		if loc := page.Header.Get("Location"); loc != "/login" {
			t.Fatalf("token %q: /chat redirects to %q, want /login", token, loc)
		}
	}
}

// TestLoginPageServesBothForms checks the combined page (R13): /login
// answers 200 with the login and registration forms, and sends signed-in
// visitors to /chat instead.
func TestLoginPageServesBothForms(t *testing.T) {
	_, ts := newTestService(t)

	resp := getWithCookie(t, ts.URL+"/login", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/login status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("/login content type %q, want text/html", ct)
	}
	page, _ := io.ReadAll(resp.Body)
	for _, want := range []string{`id="login-form"`, `id="register-form"`, "/api/login", "/api/register"} {
		if !strings.Contains(string(page), want) {
			t.Fatalf("/login page is missing %q", want)
		}
	}

	reg := postCredentials(t, ts.Client(), ts.URL, "/api/register", "frank", "pw")
	if reg.status != http.StatusOK {
		t.Fatalf("register status %d, want 200", reg.status)
	}
	authed := getWithCookie(t, ts.URL+"/login", reg.session)
	if authed.StatusCode < 300 || authed.StatusCode >= 400 {
		t.Fatalf("authed /login status %d, want a redirect", authed.StatusCode)
	}
	if loc := authed.Header.Get("Location"); loc != "/chat" {
		t.Fatalf("authed /login redirects to %q, want /chat", loc)
	}
}

// TestLogoutInvalidatesSession: the session row is deleted (the old cookie
// stops working) and the response clears the cookie.
func TestLogoutInvalidatesSession(t *testing.T) {
	_, ts := newTestService(t)
	client := &http.Client{}

	reg := postCredentials(t, client, ts.URL, "/api/register", "gina", "pw")
	if reg.status != http.StatusOK {
		t.Fatalf("register status %d, want 200", reg.status)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/logout", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: reg.session})
	resp := do(t, client, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logout status %d, want 200", resp.StatusCode)
	}
	cleared := false
	for _, c := range resp.Cookies() {
		if c.Name == SessionCookieName && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("logout did not clear the session cookie")
	}

	if me := getWithCookie(t, ts.URL+"/api/me", reg.session); me.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/api/me with the logged-out cookie: status %d, want 401", me.StatusCode)
	}
}

// TestSessionSurvivesStoreReopen closes the database behind the service and
// rebuilds the stack over the same file; the browser's cookie still works.
func TestSessionSurvivesStoreReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tpl, err := template.ParseFiles(loginTemplatePath)
	if err != nil {
		t.Fatalf("parse login template: %v", err)
	}
	fx := newFixture(t, st, tpl)
	ts := httptest.NewServer(fx.mux)

	reg := postCredentials(t, http.DefaultClient, ts.URL, "/api/register", "heidi", "pw")
	if reg.status != http.StatusOK {
		t.Fatalf("register status %d, want 200", reg.status)
	}
	ts.Close()
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	fx2 := newFixture(t, reopened, tpl)
	ts2 := httptest.NewServer(fx2.mux)
	t.Cleanup(ts2.Close)

	me := getWithCookie(t, ts2.URL+"/api/me", reg.session)
	if me.StatusCode != http.StatusOK {
		t.Fatalf("/api/me after reopen: status %d, want 200", me.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(me.Body).Decode(&body); err != nil {
		t.Fatalf("decode /api/me: %v", err)
	}
	if body["id"] != reg.body["id"] {
		t.Fatalf("session resolved to %s after reopen, want %s", body["id"], reg.body["id"])
	}
}

// TestMalformedBodyRejected keeps bad JSON from becoming a 500.
func TestMalformedBodyRejected(t *testing.T) {
	_, ts := newTestService(t)

	for _, path := range []string{"/api/register", "/api/login"} {
		resp, err := http.Post(ts.URL+path, "application/json", strings.NewReader("{not json"))
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("post %s malformed body: status %d, want 400", path, resp.StatusCode)
		}
	}
}

// TestConcurrentSamePairRegistrationExactlyOneSucceeds drives the pair rule
// through the HTTP surface: N parallel registrations of one (username,
// password) pair yield exactly one success.
func TestConcurrentSamePairRegistrationExactlyOneSucceeds(t *testing.T) {
	_, ts := newTestService(t)
	client := ts.Client()

	const n = 6
	responses := make(chan apiResponse, n)
	for i := 0; i < n; i++ {
		go func() {
			resp, err := postCredentialsHTTP(client, ts.URL, "/api/register", "ivan", "same-password")
			if err != nil {
				t.Errorf("concurrent register: %v", err)
				responses <- apiResponse{}
				return
			}
			responses <- resp
		}()
	}

	successes, conflicts := 0, 0
	for i := 0; i < n; i++ {
		resp := <-responses
		switch resp.status {
		case http.StatusOK:
			successes++
		case http.StatusConflict:
			conflicts++
		default:
			t.Errorf("unexpected status %d (error %q)", resp.status, resp.body["error"])
		}
	}
	if successes != 1 || conflicts != n-1 {
		t.Fatalf("%d successes and %d conflicts, want 1 and %d", successes, conflicts, n-1)
	}
}
