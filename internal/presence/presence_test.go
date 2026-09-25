package presence

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
	"sync"
	"testing"
	"time"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/chat"
	"github.com/usual2970/sample-im/internal/store"
)

// fakeClock is a mutable clock: TTL tests advance it instead of sleeping, so
// a 45s expiry costs nanoseconds.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// capturedPublish is one publish the fake publisher saw, with the payload
// decoded through its JSON wire shape so tests pin the field names clients
// depend on, not the internal types.
type capturedPublish struct {
	topic  string
	event  string
	raw    string
	online []User
}

// capturePublisher stands in for the pushlet: it marshals every payload the
// way PublishJSON would and records it for inspection.
type capturePublisher struct {
	mu    sync.Mutex
	calls []capturedPublish
}

func (p *capturePublisher) PublishJSON(topic, event string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, capturedPublish{topic: topic, event: event, raw: string(raw), online: snap.Online})
	return nil
}

func (p *capturePublisher) publishes() []capturedPublish {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]capturedPublish(nil), p.calls...)
}

// failingPublisher fails every publish; the 502-path test injects it.
type failingPublisher struct{ err error }

func (f failingPublisher) PublishJSON(string, string, any) error { return f.err }

// scriptedPublisher simulates a transient outage: the 1-based call numbers in
// failAt return an error, every other call records like capturePublisher.
// Sweep-retry tests script exactly one failed publish this way.
type scriptedPublisher struct {
	capturePublisher
	mu     sync.Mutex
	calls  int
	failAt map[int]bool
}

func (p *scriptedPublisher) PublishJSON(topic, event string, v any) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	fail := p.failAt[call]
	p.mu.Unlock()
	if fail {
		return fmt.Errorf("publish %d failed (scripted outage)", call)
	}
	return p.capturePublisher.PublishJSON(topic, event, v)
}

// fixture wires a store, the auth service, and one presence engine behind the
// same RequireAuth middleware main.go uses, over a fresh SQLite file. The
// engine's publisher is whatever the test built it with.
type fixture struct {
	store   *store.Store
	engine  *Engine
	ts      *httptest.Server
	capture *capturePublisher
}

func newFixture(t *testing.T, e *Engine, capture *capturePublisher) *fixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	authSvc := auth.NewService(st, template.New("empty")) // presence tests render no pages
	mux := http.NewServeMux()
	mux.Handle("POST /api/presence/join", authSvc.RequireAuth(http.HandlerFunc(e.HandleJoin)))
	mux.Handle("POST /api/presence/heartbeat", authSvc.RequireAuth(http.HandlerFunc(e.HandleHeartbeat)))
	mux.Handle("POST /api/leave", authSvc.RequireAuth(http.HandlerFunc(e.HandleLeave)))
	fx := &fixture{store: st, engine: e, ts: httptest.NewServer(mux), capture: capture}
	t.Cleanup(fx.ts.Close)
	return fx
}

// session registers username directly in the store and returns its user id
// and a session cookie token (same shortcut as the chat tests: no bcrypt
// round trip through the HTTP register endpoint).
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

// postPath POSTs path carrying the session cookie (empty token sends none)
// and returns the status with the raw body. body may be empty and no
// Content-Type is set — exactly the shape navigator.sendBeacon produces.
func postPath(t *testing.T, ts *httptest.Server, path, token, body string) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: token})
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// findUser returns the snapshot entry with the given id.
func findUser(s Snapshot, id string) (User, bool) {
	for _, u := range s.Online {
		if u.ID == id {
			return u, true
		}
	}
	return User{}, false
}

// TestSweepExpiresStaleEntriesAndPublishes pins the ghost-expiry story: an
// entry older than the TTL is dropped by one sweep cycle and the removal
// propagates as a fresh (now empty) snapshot on the room topic.
func TestSweepExpiresStaleEntriesAndPublishes(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{Now: clock.Now, TTL: 45 * time.Second})

	if _, err := e.Join("user-1", "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	if got := len(e.Snapshot().Online); got != 1 {
		t.Fatalf("after join %d online, want 1", got)
	}
	publishes := pub.publishes()
	if n := len(publishes); n != 1 {
		t.Fatalf("join published %d snapshots, want 1", n)
	}

	clock.Advance(46 * time.Second) // past the 45s TTL
	e.sweepOnce()

	if got := e.Snapshot().Online; len(got) != 0 {
		t.Fatalf("after sweep %+v online, want the stale entry dropped", got)
	}
	publishes = pub.publishes()
	if n := len(publishes); n != 2 {
		t.Fatalf("sweep published %d snapshots, want 1 more", n-1)
	}
	last := publishes[1]
	if last.topic != chat.RoomTopic {
		t.Fatalf("snapshot published to topic %q, want %q", last.topic, chat.RoomTopic)
	}
	if last.event != "presence" {
		t.Fatalf("snapshot event %q, want %q", last.event, "presence")
	}
	if len(last.online) != 0 {
		t.Fatalf("post-sweep snapshot %+v, want it empty", last.online)
	}
}

// TestHeartbeatRefreshesLiveness: a heartbeating user survives a sweep that
// would otherwise expire them, and only goes away once the heartbeats stop
// for longer than the TTL.
func TestHeartbeatRefreshesLiveness(t *testing.T) {
	clock := &fakeClock{now: time.Unix(2000, 0)}
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{Now: clock.Now, TTL: 45 * time.Second})

	if _, err := e.Join("user-1", "bob"); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Heartbeat at 40s in: 5s before expiry. Another 40s later the entry is
	// only 40s old again, so the sweep keeps it.
	clock.Advance(40 * time.Second)
	if err := e.Heartbeat("user-1", "bob"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	clock.Advance(40 * time.Second)
	e.sweepOnce()
	if got := e.Snapshot().Online; len(got) != 1 || got[0].ID != "user-1" {
		t.Fatalf("after heartbeat+sweep %+v online, want user-1 kept", got)
	}

	// No further heartbeat: 46s of silence expires the entry.
	clock.Advance(46 * time.Second)
	e.sweepOnce()
	if got := e.Snapshot().Online; len(got) != 0 {
		t.Fatalf("after silent TTL %+v online, want it expired", got)
	}
}

// TestHeartbeatRejoinsAbsentEntry pins the self-heal: a heartbeat for an id
// with no entry (TTL-swept while a throttled background tab kept beating, or
// a join that never landed) recreates the entry and publishes the comeback
// snapshot, while a heartbeat for a live entry only refreshes lastSeen and
// publishes nothing.
func TestHeartbeatRejoinsAbsentEntry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(5000, 0)}
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{Now: clock.Now, TTL: 45 * time.Second})

	// Absent id: the heartbeat acts as a join.
	if err := e.Heartbeat("user-1", "prodigal"); err != nil {
		t.Fatalf("heartbeat on absent id: %v", err)
	}
	u, ok := findUser(e.Snapshot(), "user-1")
	if !ok || u.Name != "prodigal" || u.Display != "prodigal" {
		t.Fatalf("after heartbeat entry %+v (found %v), want prodigal online", u, ok)
	}
	publishes := pub.publishes()
	if n := len(publishes); n != 1 {
		t.Fatalf("rejoin heartbeat published %d snapshots, want 1", n)
	}
	if len(publishes[0].online) != 1 || publishes[0].online[0].ID != "user-1" {
		t.Fatalf("rejoin snapshot %+v does not contain the caller", publishes[0].online)
	}

	// Live entry: the heartbeat refreshes lastSeen and stays publish-silent.
	clock.Advance(30 * time.Second)
	if err := e.Heartbeat("user-1", "prodigal"); err != nil {
		t.Fatalf("refresh heartbeat: %v", err)
	}
	if n := len(pub.publishes()); n != 1 {
		t.Fatalf("refresh heartbeat published again (%d total), want no new snapshot", n)
	}
	// 40s past the refresh (70s past the rejoin) the refreshed entry survives
	// a sweep — and the quiescent sweep adds no publish of its own.
	clock.Advance(40 * time.Second)
	e.sweepOnce()
	if u, ok := findUser(e.Snapshot(), "user-1"); !ok {
		t.Fatal("refresh heartbeat did not keep the entry alive across a sweep")
	} else if u.Display != "prodigal" {
		t.Fatalf("entry display %q changed across a sweep", u.Display)
	}
	if n := len(pub.publishes()); n != 1 {
		t.Fatalf("sweep after a refresh published again (%d total), want no new snapshot", n)
	}
}

// TestSweepKeepsFreshEntries: a sweep with nothing to drop changes nothing
// and publishes nothing.
func TestSweepKeepsFreshEntries(t *testing.T) {
	clock := &fakeClock{now: time.Unix(3000, 0)}
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{Now: clock.Now, TTL: 45 * time.Second})

	if _, err := e.Join("user-1", "carol"); err != nil {
		t.Fatalf("join: %v", err)
	}
	clock.Advance(10 * time.Second)
	e.sweepOnce()

	if got := e.Snapshot().Online; len(got) != 1 {
		t.Fatalf("fresh entry %+v after sweep, want it kept", got)
	}
	if n := len(pub.publishes()); n != 1 {
		t.Fatalf("sweep published %d extra snapshots, want none", n-1)
	}
}

// TestSweepRepublishesAfterFailedPublish pins the retry: when the sweep that
// dropped a ghost fails to publish, later quiescent sweeps republish the
// healed snapshot until one succeeds — and stop once it has, so a healthy
// room is not respammed every tick.
func TestSweepRepublishesAfterFailedPublish(t *testing.T) {
	clock := &fakeClock{now: time.Unix(6000, 0)}
	pub := &scriptedPublisher{failAt: map[int]bool{2: true}} // the removal publish fails
	e := NewEngine(pub, Config{Now: clock.Now, TTL: 45 * time.Second})

	if _, err := e.Join("user-1", "ghost"); err != nil {
		t.Fatalf("join: %v", err)
	}

	// The dropping sweep owes a publish; the relay outage swallows it.
	clock.Advance(46 * time.Second)
	e.sweepOnce()
	if got := len(e.Snapshot().Online); got != 0 {
		t.Fatalf("%d online after sweep, want the ghost dropped", got)
	}
	if n := len(pub.publishes()); n != 1 {
		t.Fatalf("%d successful publishes after the failed sweep, want only the join's", n)
	}

	// Quiescent sweep, relay back up: the owed snapshot goes out.
	e.sweepOnce()
	publishes := pub.publishes()
	if n := len(publishes); n != 2 {
		t.Fatalf("%d successful publishes after retry, want 2 (join + retried sweep)", n)
	}
	if len(publishes[1].online) != 0 {
		t.Fatalf("retried snapshot %+v, want it empty", publishes[1].online)
	}

	// Debt paid: further quiescent sweeps publish nothing.
	e.sweepOnce()
	if n := len(pub.publishes()); n != 2 {
		t.Fatalf("sweep kept republishing (%d publishes), want it to stop after success", n)
	}
}

// TestJoinAndLeavePublishSnapshots: join and leave each broadcast a full
// snapshot on the room topic containing, then dropping, the caller. A leave
// for someone already offline republishes nothing.
func TestJoinAndLeavePublishSnapshots(t *testing.T) {
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{})

	snap, err := e.Join("user-1", "dave")
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if u, ok := findUser(snap, "user-1"); !ok || u.Name != "dave" || u.Display != "dave" {
		t.Fatalf("join reply entry %+v (found %v), want dave displayed plain", u, ok)
	}

	publishes := pub.publishes()
	if n := len(publishes); n != 1 {
		t.Fatalf("join published %d snapshots, want 1", n)
	}
	if len(publishes[0].online) != 1 || publishes[0].online[0].ID != "user-1" {
		t.Fatalf("join snapshot %+v does not contain the caller", publishes[0].online)
	}
	if publishes[0].raw == "" || !strings.Contains(publishes[0].raw, `"online"`) {
		t.Fatalf("join snapshot payload %q lacks the online field", publishes[0].raw)
	}

	snap, err = e.Leave("user-1")
	if err != nil {
		t.Fatalf("leave: %v", err)
	}
	if _, ok := findUser(snap, "user-1"); ok {
		t.Fatalf("leave reply snapshot %+v still contains the caller", snap.Online)
	}
	publishes = pub.publishes()
	if n := len(publishes); n != 2 {
		t.Fatalf("leave published %d snapshots total, want 2", n)
	}
	if len(publishes[1].online) != 0 {
		t.Fatalf("leave snapshot %+v, want it empty", publishes[1].online)
	}

	// Leaving while already offline (double beacon, stale sweep) is a no-op.
	if _, err := e.Leave("user-1"); err != nil {
		t.Fatalf("second leave: %v", err)
	}
	if n := len(pub.publishes()); n != 2 {
		t.Fatalf("second leave published again (%d total), want no new snapshot", n)
	}
}

// TestDuplicateNamesGetSuffixesAndRevert pins KTD5: two concurrently online
// users sharing a username each display name#xxxx built from a short suffix
// of their user id, and the survivor reverts to the plain name once the
// other leaves.
func TestDuplicateNamesGetSuffixesAndRevert(t *testing.T) {
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{})

	erinID := "aaaaaaaaaaaaaaaaaaaa1111" // 22-char id shape, suffix "1111"
	frankID := "bbbbbbbbbbbbbbbbbbbb2222"

	snap, err := e.Join(erinID, "twin")
	if err != nil {
		t.Fatalf("join erin: %v", err)
	}
	if u, _ := findUser(snap, erinID); u.Display != "twin" {
		t.Fatalf("lone twin displays %q, want the plain name", u.Display)
	}

	snap, err = e.Join(frankID, "twin")
	if err != nil {
		t.Fatalf("join frank: %v", err)
	}
	erin, ok := findUser(snap, erinID)
	if !ok {
		t.Fatal("second join snapshot lost the first twin")
	}
	frank, ok := findUser(snap, frankID)
	if !ok {
		t.Fatal("second join snapshot lost the second twin")
	}
	if erin.Display != "twin#1111" {
		t.Fatalf("first twin displays %q, want twin#1111", erin.Display)
	}
	if frank.Display != "twin#2222" {
		t.Fatalf("second twin displays %q, want twin#2222", frank.Display)
	}
	if erin.Display == frank.Display {
		t.Fatal("duplicate-name displays are not distinct")
	}
	for _, u := range snap.Online {
		if u.Name != "twin" {
			t.Fatalf("entry %+v changed the underlying name", u)
		}
	}

	// The broadcast snapshot carries the same disambiguated names.
	publishes := pub.publishes()
	if len(publishes) == 0 {
		t.Fatal("no snapshot published")
	}
	latest := publishes[len(publishes)-1]
	if u, _ := findUser(Snapshot{Online: latest.online}, erinID); u.Display != "twin#1111" {
		t.Fatalf("broadcast display %q, want twin#1111", u.Display)
	}

	// One twin leaves; the survivor goes back to the plain name.
	snap, err = e.Leave(frankID)
	if err != nil {
		t.Fatalf("leave frank: %v", err)
	}
	if u, ok := findUser(snap, erinID); !ok || u.Display != "twin" {
		t.Fatalf("survivor entry %+v (found %v), want the plain name back", u, ok)
	}
}

// TestMultiTabJoinDoesNotDuplicate: presence is per user id, not per tab — a
// second join from the same account refreshes the one entry instead of
// adding another.
func TestMultiTabJoinDoesNotDuplicate(t *testing.T) {
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{})

	if _, err := e.Join("user-1", "solo"); err != nil {
		t.Fatalf("first join: %v", err)
	}
	snap, err := e.Join("user-1", "solo")
	if err != nil {
		t.Fatalf("second join: %v", err)
	}
	if got := len(snap.Online); got != 1 {
		t.Fatalf("second join from the same id shows %d online, want 1", got)
	}
	if u := snap.Online[0]; u.ID != "user-1" || u.Display != "solo" {
		t.Fatalf("entry %+v is not the single refreshed one", u)
	}
	for _, p := range pub.publishes() {
		if len(p.online) != 1 {
			t.Fatalf("a snapshot carried %d entries for one user id", len(p.online))
		}
	}
}

// TestSnapshotOrdersByNameThenID: snapshots come back in a stable
// (name, id) order — map iteration order must never leak to the UI.
func TestSnapshotOrdersByNameThenID(t *testing.T) {
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{})

	for _, u := range [][2]string{
		{"user-c", "zoe"},
		{"user-a", "amy"},
		{"user-b", "zoe"},
	} {
		if _, err := e.Join(u[0], u[1]); err != nil {
			t.Fatalf("join %s: %v", u[0], err)
		}
	}
	snap := e.Snapshot()
	want := []string{"user-a", "user-b", "user-c"} // amy, then zoe/user-b, zoe/user-c
	if len(snap.Online) != len(want) {
		t.Fatalf("snapshot has %d entries, want %d", len(snap.Online), len(want))
	}
	for i, id := range want {
		if snap.Online[i].ID != id {
			t.Fatalf("snapshot order [%d] is %s, want %s (full %+v)", i, snap.Online[i].ID, id, snap.Online)
		}
	}
}

// TestUnauthenticatedRequestsRejected: all three endpoints sit behind
// RequireAuth — no session cookie, no presence changes.
func TestUnauthenticatedRequestsRejected(t *testing.T) {
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{})
	fx := newFixture(t, e, pub)

	for _, path := range []string{"/api/presence/join", "/api/presence/heartbeat", "/api/leave"} {
		status, body := postPath(t, fx.ts, path, "", "")
		if status != http.StatusUnauthorized {
			t.Fatalf("anonymous POST %s status %d, want 401", path, status)
		}
		if !strings.Contains(body, "authentication required") {
			t.Fatalf("anonymous POST %s body %q lacks the error", path, body)
		}
	}
	if got := len(e.Snapshot().Online); got != 0 {
		t.Fatalf("anonymous requests left %d entries online", got)
	}
	if n := len(pub.publishes()); n != 0 {
		t.Fatalf("anonymous requests published %d snapshots", n)
	}
}

// TestHandlersServeBeaconShapedRequests walks the happy path over HTTP:
// join answers 200 with the current list as JSON, heartbeat answers 200, and
// leave accepts exactly what navigator.sendBeacon sends — a plain POST with
// an empty body and no Content-Type — removing the caller and publishing the
// shrunken snapshot.
func TestHandlersServeBeaconShapedRequests(t *testing.T) {
	clock := &fakeClock{now: time.Unix(4000, 0)}
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{Now: clock.Now, TTL: 45 * time.Second})
	fx := newFixture(t, e, pub)
	id1, token1 := session(t, fx, "wired")
	_, token2 := session(t, fx, "visitor")

	status, body := postPath(t, fx.ts, "/api/presence/join", token1, "")
	if status != http.StatusOK {
		t.Fatalf("join status %d (%s), want 200", status, body)
	}
	var joined Snapshot
	if err := json.Unmarshal([]byte(body), &joined); err != nil {
		t.Fatalf("join body %q is not a snapshot: %v", body, err)
	}
	if u, ok := findUser(joined, id1); !ok || u.Name != "wired" || u.Display != "wired" {
		t.Fatalf("join reply entry %+v (found %v), want wired", u, ok)
	}

	status, body = postPath(t, fx.ts, "/api/presence/join", token2, "")
	if status != http.StatusOK {
		t.Fatalf("second join status %d (%s), want 200", status, body)
	}

	status, body = postPath(t, fx.ts, "/api/presence/heartbeat", token1, "")
	if status != http.StatusOK {
		t.Fatalf("heartbeat status %d (%s), want 200", status, body)
	}

	// Beacon-shaped leave: empty body, no Content-Type, cookie only.
	status, body = postPath(t, fx.ts, "/api/leave", token2, "")
	if status != http.StatusOK {
		t.Fatalf("leave status %d (%s), want 200", status, body)
	}

	snap := e.Snapshot()
	if got := len(snap.Online); got != 1 {
		t.Fatalf("%d online after the beacon leave, want the leaver gone", got)
	}
	if u, ok := findUser(snap, id1); !ok {
		t.Fatalf("survivor missing from %+v", snap.Online)
	} else if u.Display != "wired" {
		t.Fatalf("survivor displays %q, want the plain name", u.Display)
	}

	publishes := pub.publishes()
	if n := len(publishes); n < 3 {
		t.Fatalf("only %d snapshots published, want one per join and leave", n)
	} else if len(publishes[n-1].online) != 1 {
		t.Fatalf("last snapshot %+v does not show exactly the survivor", publishes[n-1].online)
	}
}

// TestHeartbeatOverHTTPRejoinsWithSessionName: behind RequireAuth, a
// heartbeat with no prior join recreates the caller's entry using the
// username from the session — the empty beacon-shaped body carries no
// identity — and publishes the comeback snapshot on the room topic.
func TestHeartbeatOverHTTPRejoinsWithSessionName(t *testing.T) {
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{})
	fx := newFixture(t, e, pub)
	id, token := session(t, fx, "wanderer")

	status, body := postPath(t, fx.ts, "/api/presence/heartbeat", token, "")
	if status != http.StatusOK {
		t.Fatalf("heartbeat status %d (%s), want 200", status, body)
	}
	u, ok := findUser(e.Snapshot(), id)
	if !ok || u.Name != "wanderer" || u.Display != "wanderer" {
		t.Fatalf("after heartbeat entry %+v (found %v), want the session's username", u, ok)
	}
	publishes := pub.publishes()
	if n := len(publishes); n != 1 {
		t.Fatalf("heartbeat published %d snapshots, want 1", n)
	}
	if len(publishes[0].online) != 1 || publishes[0].online[0].ID != id {
		t.Fatalf("heartbeat snapshot %+v does not contain exactly the caller", publishes[0].online)
	}
}

// TestJoinPublishFailureAnswers502 pins the degraded-broadcast policy: the
// entry is recorded (the state change stands, and the next snapshot heals
// the room) but the reply reports the failed broadcast with 502, matching
// the chat service's synchronous-publish contract.
func TestJoinPublishFailureAnswers502(t *testing.T) {
	e := NewEngine(failingPublisher{err: fmt.Errorf("relay is down")}, Config{})
	fx := newFixture(t, e, nil)
	id, token := session(t, fx, "unlucky")

	status, body := postPath(t, fx.ts, "/api/presence/join", token, "")
	if status != http.StatusBadGateway {
		t.Fatalf("join status %d (%s), want 502", status, body)
	}
	if !strings.Contains(body, "not broadcast") {
		t.Fatalf("join error %q does not explain the degraded broadcast", body)
	}
	if u, ok := findUser(e.Snapshot(), id); !ok {
		t.Fatal("join did not record the caller despite the publish failure")
	} else if u.Display != "unlucky" {
		t.Fatalf("recorded entry %+v is wrong", u)
	}

	// Leave with the relay still down: removal stands, 502 reported.
	status, _ = postPath(t, fx.ts, "/api/leave", token, "")
	if status != http.StatusBadGateway {
		t.Fatalf("leave status %d, want 502", status)
	}
	if got := len(e.Snapshot().Online); got != 0 {
		t.Fatalf("leave did not remove the caller despite the publish failure")
	}

	// Heartbeat with the relay still down doubles as a rejoin: the entry is
	// recreated under the session's name, and the failed broadcast 502s.
	status, _ = postPath(t, fx.ts, "/api/presence/heartbeat", token, "")
	if status != http.StatusBadGateway {
		t.Fatalf("heartbeat status %d, want 502", status)
	}
	if u, ok := findUser(e.Snapshot(), id); !ok || u.Display != "unlucky" {
		t.Fatalf("heartbeat entry %+v (found %v) after 502, want unlucky re-added", u, ok)
	}
}

// TestSweeperGoroutineExpiresAndStops drives the real sweeper loop with a
// tiny TTL: the goroutine drops a silent joiner without any test calling
// sweepOnce, publishes the emptied snapshot, and Stop (twice, and on an
// engine that never started) returns without hanging.
func TestSweeperGoroutineExpiresAndStops(t *testing.T) {
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{TTL: 40 * time.Millisecond, SweepInterval: 5 * time.Millisecond})
	e.Start()
	t.Cleanup(e.Stop)

	if _, err := e.Join("user-1", "ghost"); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Nobody leaves in this test, so an empty snapshot can only come from the
	// sweeper. Poll for that empty *publish* rather than for the empty map:
	// sweepOnce deletes the entry under the engine lock but records its
	// publish only after unlocking, so the map can read empty a moment
	// before the publish lands — a window this test used to race.
	deadline := time.Now().Add(2 * time.Second)
	for !anyEmptySnapshot(pub.publishes()) {
		if time.Now().After(deadline) {
			t.Fatal("sweeper goroutine did not expire the silent joiner within 2s")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := len(e.Snapshot().Online); got != 0 {
		t.Fatalf("%d online after the sweeper's empty snapshot, want 0", got)
	}

	e.Stop()                        // second Stop must be a safe no-op
	NewEngine(pub, Config{}).Stop() // Stop before any Start must not hang
}

// anyEmptySnapshot reports whether any recorded publish carried an empty
// online list.
func anyEmptySnapshot(publishes []capturedPublish) bool {
	for _, p := range publishes {
		if len(p.online) == 0 {
			return true
		}
	}
	return false
}

// TestConcurrentJoinHeartbeatSweep hammers one engine from many goroutines
// while a separate goroutine advances the fake clock, so join, heartbeat,
// leave, snapshot, and sweep collide continuously; go test -race proves the
// locking discipline holds.
func TestConcurrentJoinHeartbeatSweep(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	pub := &capturePublisher{}
	e := NewEngine(pub, Config{Now: clock.Now, TTL: 2 * time.Second})

	var clockWG sync.WaitGroup
	stopClock := make(chan struct{})
	clockWG.Add(1)
	go func() {
		defer clockWG.Done()
		for {
			select {
			case <-stopClock:
				return
			default:
				clock.Advance(50 * time.Millisecond)
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	const workers = 8
	const iterations = 100
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				id := fmt.Sprintf("user-%d", (w+i)%6)
				_, _ = e.Join(id, "racer")
				_ = e.Heartbeat(id, "racer")
				_ = e.Snapshot()
				if i%3 == 0 {
					e.sweepOnce()
				}
				if i%5 == 0 {
					_, _ = e.Leave(id)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stopClock)
	clockWG.Wait()

	// Sanity: whatever survived is well-formed and correctly named.
	for _, u := range e.Snapshot().Online {
		if u.Name != "racer" || u.ID == "" {
			t.Fatalf("malformed entry %+v after concurrent hammering", u)
		}
		if u.Display != "racer" && !strings.HasPrefix(u.Display, "racer#") {
			t.Fatalf("entry %+v has an invalid display name", u)
		}
	}
}
