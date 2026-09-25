package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

// newTestStore opens a Store backed by a fresh SQLite file inside t.TempDir().
func newTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// openRawDB opens a second, test-only connection straight to the store's
// database file so tests can inspect rows without going through the Store API.
func openRawDB(t *testing.T, st *Store, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestRegisterRejectsExistingPair pins the pair-uniqueness rule (R3): the
// same (username, password) combination identifies exactly one account, while
// the same username with a different password registers a new account.
func TestRegisterRejectsExistingPair(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	first, err := st.Register(ctx, "alice", []byte("password-one"))
	if err != nil {
		t.Fatalf("first register: %v", err)
	}
	if _, err := st.Register(ctx, "alice", []byte("password-one")); !errors.Is(err, ErrPairExists) {
		t.Fatalf("re-registering the same pair: err = %v, want ErrPairExists", err)
	}
	second, err := st.Register(ctx, "alice", []byte("password-two"))
	if err != nil {
		t.Fatalf("same name, different password: %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("same-name accounts share id %q, want distinct ids", first.ID)
	}

	users, err := st.UsersByName(ctx, "alice")
	if err != nil {
		t.Fatalf("users by name: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("stored %d alice accounts, want 2", len(users))
	}
}

// TestSameNameAccountsResolveByPassword covers AE1: duplicate usernames with
// different passwords each log in to their own account, exactly as the auth
// layer resolves logins (compare the candidate against every same-name hash).
func TestSameNameAccountsResolveByPassword(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	first, err := st.Register(ctx, "bob", []byte("hunter2"))
	if err != nil {
		t.Fatalf("register first bob: %v", err)
	}
	second, err := st.Register(ctx, "bob", []byte("hunter3"))
	if err != nil {
		t.Fatalf("register second bob: %v", err)
	}
	if first.ID == second.ID {
		t.Fatal("accounts share a user id")
	}

	resolve := func(password string) *User {
		t.Helper()
		users, err := st.UsersByName(ctx, "bob")
		if err != nil {
			t.Fatalf("users by name: %v", err)
		}
		for _, u := range users {
			if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) == nil {
				return &u
			}
		}
		return nil
	}
	if got := resolve("hunter2"); got == nil || got.ID != first.ID {
		t.Fatalf("hunter2 resolved %+v, want first account %s", got, first.ID)
	}
	if got := resolve("hunter3"); got == nil || got.ID != second.ID {
		t.Fatalf("hunter3 resolved %+v, want second account %s", got, second.ID)
	}
	if got := resolve("hunter4"); got != nil {
		t.Fatalf("wrong password resolved account %s", got.ID)
	}
}

// TestRegisterStoresNoPlaintextPassword proves the stored credential is a
// bcrypt hash, never the raw password.
func TestRegisterStoresNoPlaintextPassword(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	const password = "super-secret-passw0rd"
	user, err := st.Register(context.Background(), "carol", []byte(password))
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	db := openRawDB(t, st, path)
	var username, hash string
	if err := db.QueryRow(`SELECT username, password_hash FROM users WHERE id = ?`, user.ID).Scan(&username, &hash); err != nil {
		t.Fatalf("query users row: %v", err)
	}
	if username != "carol" {
		t.Fatalf("stored username %q, want carol", username)
	}
	if hash == password || strings.Contains(hash, password) {
		t.Fatalf("users row stores the plaintext password: %q", hash)
	}
	if !strings.HasPrefix(hash, "$2") || len(hash) < 59 {
		t.Fatalf("stored hash %q does not look like bcrypt", hash)
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		t.Fatal("stored hash does not verify against the registered password")
	}
}

// TestRegisterMintsURLSafeIdentity checks R4: user ids and dm secrets are
// random tokens from a URL-safe alphabet, at least 22 characters long.
func TestRegisterMintsURLSafeIdentity(t *testing.T) {
	st := newTestStore(t)

	user, err := st.Register(context.Background(), "dave", []byte("pw"))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	for name, token := range map[string]string{"id": user.ID, "dm_secret": user.DMSecret} {
		if len(token) < 22 {
			t.Fatalf("%s %q is shorter than 22 chars", name, token)
		}
		for _, c := range token {
			if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
				t.Fatalf("%s %q contains non-URL-safe char %q", name, token, c)
			}
		}
	}
	if user.ID == user.DMSecret {
		t.Fatal("id and dm_secret are identical")
	}
}

// TestConcurrentSamePairRegistrationExistsOnce proves the compare-then-insert
// critical section is serialized: N concurrent registrations of the same
// (username, password) pair produce exactly one account.
func TestConcurrentSamePairRegistrationExistsOnce(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const n = 8
	start := make(chan struct{})
	results := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := st.Register(ctx, "erin", []byte("same-password"))
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes, pairRejects := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrPairExists):
			pairRejects++
		default:
			t.Fatalf("unexpected registration error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("%d concurrent same-pair registrations succeeded, want exactly 1", successes)
	}
	if pairRejects != n-1 {
		t.Fatalf("%d registrations rejected as pair collisions, want %d", pairRejects, n-1)
	}

	users, err := st.UsersByName(ctx, "erin")
	if err != nil {
		t.Fatalf("users by name: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("stored %d erin accounts, want 1", len(users))
	}
}

// TestUsersAndSessionsSurviveReopen closes the store and reopens the same
// database file; accounts and live sessions must resolve as before.
func TestUsersAndSessionsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	user, err := st.Register(ctx, "frank", []byte("pw"))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	token, err := st.NewSessionToken()
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	if err := st.CreateSession(ctx, token, user.ID); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	byID, err := reopened.UserByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("user by id after reopen: %v", err)
	}
	if byID == nil || byID.Username != "frank" {
		t.Fatalf("user by id after reopen: %+v, want frank", byID)
	}
	bySession, err := reopened.UserBySession(ctx, token)
	if err != nil {
		t.Fatalf("user by session after reopen: %v", err)
	}
	if bySession == nil || bySession.ID != user.ID {
		t.Fatalf("old cookie resolved to %+v, want user %s", bySession, user.ID)
	}
}

// TestSessionLifecycle covers create, resolve, and delete, including the
// middleware's not-found contract (nil user, nil error).
func TestSessionLifecycle(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	user, err := st.Register(ctx, "gina", []byte("pw"))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	token, err := st.NewSessionToken()
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	if err := st.CreateSession(ctx, token, user.ID); err != nil {
		t.Fatalf("create session: %v", err)
	}

	resolved, err := st.UserBySession(ctx, token)
	if err != nil {
		t.Fatalf("resolve session: %v", err)
	}
	if resolved == nil || resolved.ID != user.ID {
		t.Fatalf("session resolved to %+v, want user %s", resolved, user.ID)
	}

	if err := st.DeleteSession(ctx, token); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	resolved, err = st.UserBySession(ctx, token)
	if err != nil {
		t.Fatalf("resolve deleted session: %v", err)
	}
	if resolved != nil {
		t.Fatalf("deleted session resolved to %+v, want nil", resolved)
	}
}

// TestMessagesTableReadyForChat pins the schema U3/U5 build on: the messages
// table accepts rows and carries the (scope, id) index.
func TestMessagesTableReadyForChat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	db := openRawDB(t, st, path)
	_, err = db.Exec(`INSERT INTO messages (scope, author_id, author_name, body, created_at) VALUES ('room:1', 'u1', 'alice', 'hello', 123)`)
	if err != nil {
		t.Fatalf("insert message: %v", err)
	}
	var body string
	if err := db.QueryRow(`SELECT body FROM messages WHERE scope = 'room:1' AND id = 1`).Scan(&body); err != nil {
		t.Fatalf("query message by (scope, id): %v", err)
	}
	if body != "hello" {
		t.Fatalf("message body %q, want hello", body)
	}
	var idx int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_messages_scope_id'`).Scan(&idx); err != nil {
		t.Fatalf("query index existence: %v", err)
	}
	if idx != 1 {
		t.Fatal("idx_messages_scope_id is missing")
	}
}

// TestAppendAndRecentMessages covers the message persistence U3 builds on:
// append assigns increasing ids, and recent queries filter by after-id, cap
// at the most recent rows, order oldest-first, and stay inside their scope.
func TestAppendAndRecentMessages(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		msg, err := st.AppendMessage(ctx, "room", "u1", "alice", fmt.Sprintf("m%d", i), int64(1000+i))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if msg.ID != int64(i) {
			t.Fatalf("append %d assigned id %d", i, msg.ID)
		}
	}

	recent, err := st.RecentMessages(ctx, "room", 0, 50)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(recent) != 3 || recent[0].ID != 1 || recent[2].ID != 3 {
		t.Fatalf("recent ids %v, want 1..3 oldest-first", messageIDs(recent))
	}

	after, err := st.RecentMessages(ctx, "room", 1, 50)
	if err != nil {
		t.Fatalf("recent after 1: %v", err)
	}
	if len(after) != 2 || after[0].ID != 2 || after[1].ID != 3 {
		t.Fatalf("after=1 ids %v, want 2,3", messageIDs(after))
	}

	capped, err := st.RecentMessages(ctx, "room", 0, 2)
	if err != nil {
		t.Fatalf("recent capped: %v", err)
	}
	if len(capped) != 2 || capped[0].ID != 2 || capped[1].ID != 3 {
		t.Fatalf("capped ids %v, want the most recent two 2,3", messageIDs(capped))
	}

	other, err := st.RecentMessages(ctx, "dm:u1/u2", 0, 50)
	if err != nil {
		t.Fatalf("recent other scope: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("other scope returned %d messages, want scope isolation", len(other))
	}
}

// messageIDs extracts the id column for failure messages.
func messageIDs(messages []Message) []int64 {
	ids := make([]int64, len(messages))
	for i, m := range messages {
		ids[i] = m.ID
	}
	return ids
}

// TestOpenIsIdempotent re-opens an already-migrated database file.
func TestOpenIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	for i := 0; i < 2; i++ {
		st, err := Open(path)
		if err != nil {
			t.Fatalf("open %d: %v", i+1, err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close %d: %v", i+1, err)
		}
	}
}
