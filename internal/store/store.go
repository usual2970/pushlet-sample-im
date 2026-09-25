// Package store persists sample-im's application data — accounts, login
// sessions, and chat messages — in a single SQLite file kept separate from
// the novaque relay database.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

// ErrPairExists reports a registration whose (username, password) pair
// already belongs to an existing account. Usernames may duplicate freely,
// but one name combined with one password identifies exactly one account.
var ErrPairExists = fmt.Errorf("an account with this username and password already exists")

// RoomTopic is the fixed pushlet topic the whole room UI shares: every
// global-room chat message and every presence snapshot is published on it,
// by the chat and presence packages respectively. RoomScope is the matching
// messages-table scope room history is stored under. Direct messages use
// dm:-prefixed topics and scopes instead (see [DMTopic]).
const (
	RoomTopic = "room"
	RoomScope = "room"
)

// DMTopicPrefix marks direct-message topics and conversation scopes. Topic
// names identify private conversations, so they must never appear in logs
// (the main package's logger redacts them).
const DMTopicPrefix = "dm:"

// DMTopic returns the pushlet topic carrying one user's direct messages:
// the dm: prefix plus the account's dm secret. Only the owning browser
// learns it, via /api/me; senders hand the server a user id and the server
// resolves the topic from the account record.
func DMTopic(dmSecret string) string {
	return DMTopicPrefix + dmSecret
}

// User is one registered account.
type User struct {
	ID           string
	Username     string
	PasswordHash string
	DMSecret     string
	CreatedAt    int64
}

// schema is applied idempotently on every [Open]. The messages table is
// created here already so chat and direct messages build on it
// without their own migration step.
const schema = `
CREATE TABLE IF NOT EXISTS users (
	id            TEXT PRIMARY KEY,
	username      TEXT NOT NULL,
	password_hash TEXT NOT NULL,
	dm_secret     TEXT NOT NULL,
	created_at    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
	token      TEXT PRIMARY KEY,
	user_id    TEXT NOT NULL REFERENCES users(id),
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	scope       TEXT NOT NULL,
	author_id   TEXT NOT NULL,
	author_name TEXT NOT NULL,
	body        TEXT NOT NULL,
	created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_scope_id ON messages (scope, id);
`

// Store owns the application SQLite database (accounts, sessions, messages).
// Construct it with [Open] and pair every successful open with [Close].
type Store struct {
	db *sql.DB

	// regMu serializes registration's compare-then-insert critical section.
	// Pair uniqueness cannot be a unique index (bcrypt hashes are salted), so
	// Register compares the candidate password against every same-name
	// account and then inserts while holding regMu — otherwise two
	// concurrent registrations of the same pair could both pass the compare
	// and both insert.
	regMu sync.Mutex
}

// Open opens (creating if needed) the SQLite database at path, applies the
// schema, and verifies the file is reachable. The DSN mirrors the relay
// database's pragma shape: enforced foreign keys, WAL journaling, and a 10s
// busy timeout.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	dsn := "file:" + path + "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database handle. It is safe to call more than
// once.
func (s *Store) Close() error {
	return s.db.Close()
}

// Register creates a new account for username/password unless an existing
// account already has both the same username and the same password, in which
// case it returns [ErrPairExists]. Usernames may duplicate (the pair rule is
// the only uniqueness constraint); ids and dm secrets are freshly minted
// random tokens, and the password is stored only as a bcrypt hash. The
// same-name comparison and the insert run under one lock so concurrent
// registrations of the same pair cannot both succeed.
func (s *Store) Register(ctx context.Context, username string, password []byte) (*User, error) {
	s.regMu.Lock()
	defer s.regMu.Unlock()

	sameName, err := s.UsersByName(ctx, username)
	if err != nil {
		return nil, err
	}
	for i := range sameName {
		if bcrypt.CompareHashAndPassword([]byte(sameName[i].PasswordHash), password) == nil {
			return nil, ErrPairExists
		}
	}

	hash, err := bcrypt.GenerateFromPassword(password, bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}
	id, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("mint user id: %w", err)
	}
	dmSecret, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("mint dm secret: %w", err)
	}
	user := &User{
		ID:           id,
		Username:     username,
		PasswordHash: string(hash),
		DMSecret:     dmSecret,
		CreatedAt:    time.Now().Unix(),
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO users (id, username, password_hash, dm_secret, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		user.ID, user.Username, user.PasswordHash, user.DMSecret, user.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert user: %w", err)
	}
	return user, nil
}

// UsersByName returns every account registered under username, oldest first.
// Several accounts may share a name; login disambiguates by password.
func (s *Store) UsersByName(ctx context.Context, username string) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, username, password_hash, dm_secret, created_at
		FROM users WHERE username = ? ORDER BY created_at, id`, username)
	if err != nil {
		return nil, fmt.Errorf("query users by name: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.DMSecret, &u.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// UserByID returns the account with the given id, or (nil, nil) when no such
// account exists.
func (s *Store) UserByID(ctx context.Context, id string) (*User, error) {
	var u User
	err := s.db.QueryRowContext(ctx, `
		SELECT id, username, dm_secret, created_at FROM users WHERE id = ?`, id).
		Scan(&u.ID, &u.Username, &u.DMSecret, &u.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query user by id: %w", err)
	}
	return &u, nil
}

// Message is one stored chat message. Scope names the conversation the
// message belongs to — the global room ("room") or a direct-message
// conversation — and ID is unique across the table.
type Message struct {
	ID         int64
	Scope      string
	AuthorID   string
	AuthorName string
	Body       string
	CreatedAt  int64
}

// AppendMessage inserts one message row in scope and returns it with the id
// SQLite assigned. The author identity and timestamp are stamped by the
// caller; the store only persists them.
func (s *Store) AppendMessage(ctx context.Context, scope, authorID, authorName, body string, createdAt int64) (*Message, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO messages (scope, author_id, author_name, body, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		scope, authorID, authorName, body, createdAt)
	if err != nil {
		return nil, fmt.Errorf("insert message: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("message id: %w", err)
	}
	return &Message{
		ID:         id,
		Scope:      scope,
		AuthorID:   authorID,
		AuthorName: authorName,
		Body:       body,
		CreatedAt:  createdAt,
	}, nil
}

// RecentMessages returns up to limit messages in scope with ids greater than
// afterID — the most recent ones when more than limit match — ordered
// oldest-first. afterID zero reads plain recent history. The (scope, id)
// index backs the lookup.
func (s *Store) RecentMessages(ctx context.Context, scope string, afterID int64, limit int) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, scope, author_id, author_name, body, created_at
		FROM messages
		WHERE scope = ? AND id > ?
		ORDER BY id DESC
		LIMIT ?`, scope, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Scope, &m.AuthorID, &m.AuthorName, &m.Body, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}

	// The query walks newest-first; callers read oldest-first.
	slices.Reverse(messages)
	return messages, nil
}

// NewSessionToken mints the random cookie value for a login session. Tokens
// use the same URL-safe alphabet as user ids.
func (s *Store) NewSessionToken() (string, error) {
	return randomToken()
}

// CreateSession persists a login session token for user.
func (s *Store) CreateSession(ctx context.Context, token, userID string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)`,
		token, userID, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	return nil
}

// UserBySession resolves a session token to its account, or (nil, nil) when
// the token is unknown. Callers distinguish that anonymous case from a
// non-nil error: the auth middleware answers (nil, nil) with its anonymous
// reply (401 for /api paths, a /login redirect for pages) and a lookup
// failure with 500, so a store fault never masquerades as a logout.
func (s *Store) UserBySession(ctx context.Context, token string) (*User, error) {
	var u User
	err := s.db.QueryRowContext(ctx, `
		SELECT u.id, u.username, u.dm_secret, u.created_at
		FROM sessions s JOIN users u ON u.id = s.user_id
		WHERE s.token = ?`, token).
		Scan(&u.ID, &u.Username, &u.DMSecret, &u.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query session: %w", err)
	}
	return &u, nil
}

// DeleteSession removes a login session, logging the user out.
func (s *Store) DeleteSession(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token = ?`, token)
	if err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	return nil
}

// randomToken returns a 22-character token drawn from crypto/rand and encoded
// URL-safe: 128 bits of entropy, safe for cookies, ids, and topic names.
func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
