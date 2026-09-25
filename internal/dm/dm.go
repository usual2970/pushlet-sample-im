// Package dm implements sample-im's one-to-one direct messages: every
// user owns a private pushlet topic derived from their dm secret, and
// the server resolves a recipient's topic from their account record — senders
// only ever hand over a user id. Each DM is persisted first under the pair's
// conversation scope, then published to the recipient's topic and to the
// sender's own topic, so both sides render through one stream-driven path.
package dm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/store"
)

// messageEvent is the pushlet event name carried by direct messages,
// mirroring the room's so both streams read the same shape.
const messageEvent = "message"

// Limits of the send API and history reads, matching the room's rules.
const (
	// maxBodyBytes bounds one message body after trimming: 1..2000 bytes.
	maxBodyBytes = 2000

	// historyLimit caps how many messages one history read returns.
	historyLimit = 50

	// maxRequestBytes caps the whole POST body well above maxBodyBytes so
	// oversized payloads fail fast instead of being buffered whole.
	maxRequestBytes = 16 << 10
)

// Publisher is the push surface dm needs, mirroring chat.Publisher.
// Production wires *pushlet.Pushlet; tests substitute a failing
// implementation for the 502 path.
type Publisher interface {
	PublishJSON(topic, event string, v any) error
}

// Presence is the online check the send path needs; *presence.Engine
// satisfies it with IsOnline.
type Presence interface {
	IsOnline(userID string) bool
}

// Message is the wire shape of one direct message: the 201 reply of
// POST /api/dm, the payload published on both private dm topics, and one row
// of GET /api/dm?with=<id>. Scope names the conversation
// (dm:<min(idA,idB)>:<max(idA,idB)>); To is the recipient's user id, which
// lets each side's client derive the correspondent from one shared payload:
// the peer is the author unless the author is me, in which case it is To.
type Message struct {
	ID         int64  `json:"id"`
	Scope      string `json:"scope"`
	AuthorID   string `json:"author_id"`
	AuthorName string `json:"author_name"`
	To         string `json:"to"`
	Body       string `json:"body"`
	CreatedAt  int64  `json:"created_at"`
}

// Service serves the direct-message HTTP surface over a [store.Store], a
// [Publisher], and a [Presence] check: the send endpoint, the
// caller-relative history read, and the public user lookup DM headers
// render. The caller mounts every handler behind [auth.Service.RequireAuth].
type Service struct {
	store  *store.Store
	pub    Publisher
	online Presence
}

// NewService returns a Service persisting direct messages in st, publishing
// them through pub, and gating sends on online (the presence engine).
func NewService(st *store.Store, pub Publisher, online Presence) *Service {
	return &Service{store: st, pub: pub, online: online}
}

// HandleSend accepts JSON {to, body}: it validates the body with the room's
// 1..2000-byte-after-trim rule, requires the recipient to exist (404), be
// someone other than the caller (400 — the UI never offers self-DMs, and
// honoring one would create a dead dm:<id>:<id> conversation), and be
// online per the presence engine (409 — the UI picks recipients from the
// online list, so an offline recipient means stale presence), persists the
// message under the pair's conversation scope with the author stamped, then
// publishes the payload to the recipient's private dm topic and to the
// sender's own (the sender renders their copy from their stream). The
// publishes are synchronous; the 201 reply carries the same payload
// subscribers receive.
func (s *Service) HandleSend(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.FromContext(r.Context())
	if !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	var req struct {
		To   string `json:"to"`
		Body string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		auth.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.To = strings.TrimSpace(req.To)
	body := strings.TrimSpace(req.Body)
	switch {
	case req.To == "":
		auth.WriteError(w, http.StatusBadRequest, "recipient is required")
		return
	case body == "":
		auth.WriteError(w, http.StatusBadRequest, "message body is required")
		return
	case len(body) > maxBodyBytes:
		auth.WriteError(w, http.StatusBadRequest, fmt.Sprintf("message body must be at most %d bytes", maxBodyBytes))
		return
	}

	recipient, err := s.store.UserByID(r.Context(), req.To)
	if err != nil {
		auth.WriteError(w, http.StatusInternalServerError, "could not look up the recipient")
		return
	}
	if recipient == nil {
		auth.WriteError(w, http.StatusNotFound, "recipient not found")
		return
	}
	if recipient.ID == user.ID {
		auth.WriteError(w, http.StatusBadRequest, "cannot send a direct message to yourself")
		return
	}
	if !s.online.IsOnline(recipient.ID) {
		auth.WriteError(w, http.StatusConflict, "recipient is offline; pick someone from the online list")
		return
	}

	stored, err := s.store.AppendMessage(r.Context(), conversationScope(user.ID, recipient.ID), user.ID, user.Username, body, time.Now().Unix())
	if err != nil {
		auth.WriteError(w, http.StatusInternalServerError, "could not save the message")
		return
	}
	msg := Message{
		ID:         stored.ID,
		Scope:      stored.Scope,
		AuthorID:   stored.AuthorID,
		AuthorName: stored.AuthorName,
		To:         recipient.ID,
		Body:       stored.Body,
		CreatedAt:  stored.CreatedAt,
	}

	// The recipient's copy first, then the sender's own; the row stays even
	// when a publish fails (the conversation history still delivers it),
	// mirroring the room's policy.
	if err := s.pub.PublishJSON(store.DMTopic(recipient.DMSecret), messageEvent, msg); err != nil {
		auth.WriteError(w, http.StatusBadGateway, "message saved but not broadcast; it will appear when the conversation is opened")
		return
	}
	if err := s.pub.PublishJSON(store.DMTopic(user.DMSecret), messageEvent, msg); err != nil {
		auth.WriteError(w, http.StatusBadGateway, "message saved but not broadcast; it will appear when the conversation is opened")
		return
	}
	auth.WriteJSON(w, http.StatusCreated, msg)
}

// HandleHistory answers GET /api/dm?with=<user id> with the conversation
// between the authenticated caller and that user: most recent 50 rows,
// oldest-first. Participant authorization holds by construction — the scope
// is always derived from the pair (caller, with), so a third user's query
// reads their own conversation with the named user and never anyone else's
// exchange. The with user must exist (404 otherwise).
func (s *Service) HandleHistory(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.FromContext(r.Context())
	if !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	with := strings.TrimSpace(r.URL.Query().Get("with"))
	if with == "" {
		auth.WriteError(w, http.StatusBadRequest, "with is required (the other user's id)")
		return
	}
	peer, err := s.store.UserByID(r.Context(), with)
	if err != nil {
		auth.WriteError(w, http.StatusInternalServerError, "could not look up the user")
		return
	}
	if peer == nil {
		auth.WriteError(w, http.StatusNotFound, "user not found")
		return
	}
	rows, err := s.store.RecentMessages(r.Context(), conversationScope(user.ID, peer.ID), 0, historyLimit)
	if err != nil {
		auth.WriteError(w, http.StatusInternalServerError, "could not load the conversation")
		return
	}
	auth.WriteJSON(w, http.StatusOK, toWire(rows, [2]string{user.ID, peer.ID}))
}

// HandleUser answers GET /api/users/{id} with the minimal public info DM
// headers need — id, username, and display name (equal to the username in
// this demo; accounts carry no separate profile). Nothing private, and no dm
// secret, ever leaves this handler.
func (s *Service) HandleUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := auth.FromContext(r.Context()); !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	user, err := s.store.UserByID(r.Context(), r.PathValue("id"))
	if err != nil {
		auth.WriteError(w, http.StatusInternalServerError, "could not look up the user")
		return
	}
	if user == nil {
		auth.WriteError(w, http.StatusNotFound, "user not found")
		return
	}
	auth.WriteJSON(w, http.StatusOK, map[string]string{
		"id":           user.ID,
		"username":     user.Username,
		"display_name": user.Username,
	})
}

// conversationScope derives the messages-table scope for the pair (a, b):
// dm:<min>:<max> with the ids compared lexicographically — both are
// fixed-width base64url tokens, so byte order is a stable total order and
// the scope is independent of who sent. The caller is always one of the two
// ids, which is what makes non-participant reads impossible.
func conversationScope(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return store.DMTopicPrefix + a + ":" + b
}

// toWire maps stored rows onto the wire shape. The recipient of each row is
// derived from the pair the query was scoped to: whichever of the two
// participants did not author it. The result stays non-nil so JSON answers
// render as [] rather than null.
func toWire(rows []store.Message, pair [2]string) []Message {
	messages := make([]Message, 0, len(rows))
	for _, m := range rows {
		to := ""
		switch m.AuthorID {
		case pair[0]:
			to = pair[1]
		case pair[1]:
			to = pair[0]
		}
		messages = append(messages, Message{
			ID:         m.ID,
			Scope:      m.Scope,
			AuthorID:   m.AuthorID,
			AuthorName: m.AuthorName,
			To:         to,
			Body:       m.Body,
			CreatedAt:  m.CreatedAt,
		})
	}
	return messages
}
