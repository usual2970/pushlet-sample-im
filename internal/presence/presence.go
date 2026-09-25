// Package presence implements sample-im's application-level online tracking:
// pushlet exports no connect/disconnect hooks, so the app owns
// presence itself. Clients announce themselves on every stream (re)connect,
// keep their entry alive with heartbeats (which also re-announce an entry
// lost to a sweep), and bow out with a sendBeacon on page unload; a sweeper
// goroutine expires whatever stopped beating, so a crashed client shows
// online for at most the TTL.
package presence

import (
	"cmp"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/store"
)

// presenceEvent is the pushlet event name every presence snapshot is
// published under on the chat room topic.
const presenceEvent = "presence"

// Timing defaults. The client heartbeats every ~15s (app.js); three missed
// beats plus sweep granularity bound how long a ghost lingers.
const (
	defaultTTL          = 45 * time.Second
	defaultSweepEvery   = 10 * time.Second
	displaySuffixLength = 4
)

// Publisher is the push surface presence needs, mirroring chat.Publisher.
// Production wires *pushlet.Pushlet; tests substitute fakes.
type Publisher interface {
	PublishJSON(topic, event string, v any) error
}

// User is one entry of the online list: the wire shape of the snapshot
// broadcast on the room topic and returned by the join endpoint. Name is the
// account's username; Display is what the UI shows — the plain name, or
// name#xxxx disambiguating one of several concurrently online users who
// share that name, computed server-side so every client agrees.
type User struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Display string `json:"display"`
}

// Snapshot is the full online list. Every state change republishes the
// whole snapshot rather than a delta: full snapshots are idempotent, so a
// client that misses or replays one still converges.
type Snapshot struct {
	Online []User `json:"online"`
}

// entry is one live user inside the engine's map.
type entry struct {
	name     string
	lastSeen time.Time
}

// Config overrides engine timing. Zero-valued fields fall back to the
// package defaults; tests inject a fake clock and short TTLs so a 45s
// expiry costs nanoseconds instead of a real wait.
type Config struct {
	// TTL is how long an entry survives without a join or heartbeat.
	TTL time.Duration

	// SweepInterval is how often the sweeper goroutine looks for expired
	// entries once the engine is started.
	SweepInterval time.Duration

	// Now replaces the wall clock. It must be safe for concurrent use.
	Now func() time.Time
}

// Engine tracks who is online in a map of user id → (name, lastSeen) behind
// a mutex, and publishes a snapshot to the room topic on every join, leave,
// ghost expiry, and heartbeat that re-adds a lost entry. Construct it with
// [NewEngine], mount the handlers behind [auth.Service.RequireAuth], and pair
// [Engine.Start] (the sweeper) with [Engine.Stop]. Multiple tabs of one
// account share a single entry because the map is keyed by user id.
type Engine struct {
	pub        Publisher
	ttl        time.Duration
	sweepEvery time.Duration
	now        func() time.Time
	online     map[string]*entry
	// sweepDirty is set the moment a sweep removes an entry and cleared only
	// after a sweep publish succeeds, so a failed post-removal publish is
	// retried by later sweeps instead of being silently dropped. Guarded by
	// mu; an extra retry publish is harmless because snapshots are idempotent.
	sweepDirty  bool
	mu          sync.Mutex
	startOnce   sync.Once
	stopOnce    sync.Once
	sweeperWG   sync.WaitGroup
	sweeperStop chan struct{}
}

// NewEngine returns an Engine publishing snapshots through pub. The caller
// keeps ownership of pub (pushlet outlives the engine in main).
func NewEngine(pub Publisher, cfg Config) *Engine {
	e := &Engine{
		pub:         pub,
		ttl:         defaultTTL,
		sweepEvery:  defaultSweepEvery,
		now:         time.Now,
		online:      make(map[string]*entry),
		sweeperStop: make(chan struct{}),
	}
	if cfg.TTL > 0 {
		e.ttl = cfg.TTL
	}
	if cfg.SweepInterval > 0 {
		e.sweepEvery = cfg.SweepInterval
	}
	if cfg.Now != nil {
		e.now = cfg.Now
	}
	return e
}

// Start launches the sweeper goroutine. It is safe to call at most once per
// engine; calling it after Stop is a no-op.
func (e *Engine) Start() {
	e.startOnce.Do(func() {
		select {
		case <-e.sweeperStop:
			return // already stopped; never start
		default:
		}
		e.sweeperWG.Add(1)
		go e.sweepLoop()
	})
}

// Stop halts the sweeper goroutine and waits for it to exit. It is safe to
// call more than once, and safe to call on an engine that never started.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		close(e.sweeperStop)
		e.sweeperWG.Wait()
	})
}

// sweepLoop is the sweeper goroutine body: one sweep per tick until Stop.
func (e *Engine) sweepLoop() {
	defer e.sweeperWG.Done()
	ticker := time.NewTicker(e.sweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.sweepOnce()
		case <-e.sweeperStop:
			return
		}
	}
}

// sweepOnce drops every entry whose lastSeen is older than the TTL. When
// anything was dropped it publishes the snapshot so ghost expiry propagates
// to connected clients. A failed publish leaves the sweeper dirty: later
// sweeps — even quiescent ones with nothing to drop — republish the current
// (idempotent) snapshot until one succeeds, so a quiet room still converges
// on the healed list instead of waiting for the next join or leave.
func (e *Engine) sweepOnce() {
	e.mu.Lock()
	now := e.now()
	for id, ent := range e.online {
		if now.Sub(ent.lastSeen) > e.ttl {
			delete(e.online, id)
			e.sweepDirty = true
		}
	}
	if !e.sweepDirty {
		e.mu.Unlock()
		return
	}
	snap := e.snapshotLocked()
	e.mu.Unlock()
	if err := e.publish(snap); err != nil {
		return // still dirty: the next sweep retries with a fresh snapshot
	}
	e.mu.Lock()
	e.sweepDirty = false
	e.mu.Unlock()
}

// Join records the caller as online, refreshing lastSeen for an
// already-online user (a second tab of the same account shares the entry).
// It returns the snapshot after the join, and the error from publishing it
// — the entry stands either way; callers decide how to report the failure.
func (e *Engine) Join(userID, username string) (Snapshot, error) {
	e.mu.Lock()
	e.online[userID] = &entry{name: username, lastSeen: e.now()}
	snap := e.snapshotLocked()
	e.mu.Unlock()
	return snap, e.publish(snap)
}

// Heartbeat refreshes the caller's lastSeen, keeping a quiet-but-alive tab
// online. A heartbeat for an id with no entry (TTL-swept while a throttled
// background tab missed its beats, or a join that never landed) re-adds it:
// heartbeats stop when the client truly goes away, so the upsert cannot
// outlive the TTL. A re-added entry is announced with a snapshot publish —
// the same transition a join makes — and the publish error is returned so
// callers can report the degraded broadcast; a plain refresh publishes
// nothing.
func (e *Engine) Heartbeat(userID, username string) error {
	e.mu.Lock()
	if ent, ok := e.online[userID]; ok {
		ent.lastSeen = e.now()
		e.mu.Unlock()
		return nil
	}
	e.online[userID] = &entry{name: username, lastSeen: e.now()}
	snap := e.snapshotLocked()
	e.mu.Unlock()
	return e.publish(snap)
}

// Leave removes the caller. Nothing is published when the caller was not
// online — a duplicate beacon (pagehide after beforeunload) or a leave
// racing the sweeper must not resurrect an empty update.
func (e *Engine) Leave(userID string) (Snapshot, error) {
	e.mu.Lock()
	if _, ok := e.online[userID]; !ok {
		snap := e.snapshotLocked()
		e.mu.Unlock()
		return snap, nil
	}
	delete(e.online, userID)
	snap := e.snapshotLocked()
	e.mu.Unlock()
	return snap, e.publish(snap)
}

// Snapshot returns the current online list with display names computed.
func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotLocked()
}

// IsOnline reports whether userID currently has a live entry — the check the
// direct-message send path gates recipients on.
func (e *Engine) IsOnline(userID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.online[userID]
	return ok
}

// snapshotLocked builds the wire snapshot; the caller holds e.mu. Entries
// are ordered by (name, id) so map iteration order never reaches a client,
// and every user sharing a name with another concurrently online user gets
// the name#id-suffix display while a lone name stays plain.
func (e *Engine) snapshotLocked() Snapshot {
	nameCounts := make(map[string]int, len(e.online))
	for _, ent := range e.online {
		nameCounts[ent.name]++
	}

	users := make([]User, 0, len(e.online))
	for id, ent := range e.online {
		display := ent.name
		if nameCounts[ent.name] > 1 {
			display = ent.name + "#" + idSuffix(id)
		}
		users = append(users, User{ID: id, Name: ent.name, Display: display})
	}
	slices.SortFunc(users, func(a, b User) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
	})
	return Snapshot{Online: users}
}

// idSuffix returns the trailing characters of a user id — a short, stable,
// per-account disambiguator for duplicate display names. Ids are 22-char
// base64url tokens, so the suffix is URL-safe and collision-unlikely for
// demo scale.
func idSuffix(id string) string {
	if len(id) <= displaySuffixLength {
		return id
	}
	return id[len(id)-displaySuffixLength:]
}

// publish broadcasts one snapshot on the room topic.
func (e *Engine) publish(snap Snapshot) error {
	return e.pub.PublishJSON(store.RoomTopic, presenceEvent, snap)
}

// HandleJoin serves POST /api/presence/join: it records the caller, refreshes
// lastSeen, publishes the snapshot, and answers with that same snapshot so
// the joining client can render the list before any stream event arrives.
// The request body is ignored — identity comes from the session.
func (e *Engine) HandleJoin(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.FromContext(r.Context())
	if !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	snap, err := e.Join(user.ID, user.Username)
	if err != nil {
		// The entry stands (the next snapshot heals the room); only the live
		// broadcast degraded — the same policy chat applies to messages.
		auth.WriteError(w, http.StatusBadGateway, "joined but not broadcast; the list heals on the next update")
		return
	}
	auth.WriteJSON(w, http.StatusOK, snap)
}

// HandleHeartbeat serves POST /api/presence/heartbeat: it refreshes the
// caller's lastSeen and answers 200. The body is ignored — identity, and the
// username should the heartbeat double as a rejoin, come from the session,
// exactly like join. When the heartbeat re-adds a lost entry and that
// broadcast fails, the entry still stands and the reply reports the degraded
// broadcast with 502, matching join and leave.
func (e *Engine) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.FromContext(r.Context())
	if !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if err := e.Heartbeat(user.ID, user.Username); err != nil {
		auth.WriteError(w, http.StatusBadGateway, "online but not broadcast; the list heals on the next update")
		return
	}
	auth.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// HandleLeave serves POST /api/presence/leave. It must work with
// navigator.sendBeacon: beacons are plain same-origin POSTs whose cookies
// are sent, whose body is typically empty, and whose response nobody reads
// — so this handler never touches the request body and always answers
// plainly. It removes the caller and publishes the shrunken snapshot.
func (e *Engine) HandleLeave(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.FromContext(r.Context())
	if !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if _, err := e.Leave(user.ID); err != nil {
		auth.WriteError(w, http.StatusBadGateway, "left but not broadcast; the list heals on the next update")
		return
	}
	auth.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
