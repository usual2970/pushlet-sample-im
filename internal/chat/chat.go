// Package chat serves sample-im's global chat room: the server-rendered room
// page with recent history, the posting API, and the history endpoint
// clients backfill from when their stream (re)connects. Every message is
// persisted first and then broadcast synchronously on the fixed room topic.
package chat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/store"
)

// messageEvent is the pushlet event name carried by room messages. The
// browser's EventSource dispatches it to its "message" listener.
const messageEvent = "message"

// Limits of the posting API and history reads.
const (
	// maxBodyBytes bounds one message body after trimming: 1..2000 bytes.
	maxBodyBytes = 2000

	// historyLimit caps how many messages one history read returns.
	historyLimit = 50

	// maxRequestBytes caps the whole POST body well above maxBodyBytes so
	// oversized payloads fail fast instead of being buffered whole.
	maxRequestBytes = 16 << 10
)

// chatTemplateName is the room page template (embedded by the main package).
const chatTemplateName = "chat.html"

// Publisher is the push surface chat needs. Production wires
// *pushlet.Pushlet; tests substitute a failing implementation for the 502
// path.
type Publisher interface {
	PublishJSON(topic, event string, v any) error
}

// Message is the wire shape of one room message: the 201 reply of
// POST /api/messages, the payload broadcast on the room topic, and one row
// of GET /api/messages.
type Message struct {
	ID         int64  `json:"id"`
	AuthorID   string `json:"author_id"`
	AuthorName string `json:"author_name"`
	Body       string `json:"body"`
	CreatedAt  int64  `json:"created_at"`
}

// Service serves the chat HTTP surface over a [store.Store] and a
// [Publisher]: the room page, the posting endpoint, and history reads. The
// caller mounts every handler behind [auth.Service.RequireAuth].
type Service struct {
	store *store.Store
	pub   Publisher
	tpl   *template.Template
}

// NewService returns a Service persisting room messages in st, broadcasting
// them through pub, and rendering the room page with tpl (parsed from
// chat.html by the caller).
func NewService(st *store.Store, pub Publisher, tpl *template.Template) *Service {
	return &Service{store: st, pub: pub, tpl: tpl}
}

// chatPage is the room page's template data: who is signed in and the most
// recent history rendered server-side.
type chatPage struct {
	Username string
	MeID     string
	Messages []Message
}

// HandleChatPage serves the room page: the signed-in user's name in
// the header, the most recent room messages rendered server-side through
// html/template's contextual autoescaping, and the shell (composer,
// reconnect banner, online-list container) the client script wires up.
func (s *Service) HandleChatPage(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.FromContext(r.Context())
	if !ok {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	rows, err := s.store.RecentMessages(r.Context(), store.RoomScope, 0, historyLimit)
	if err != nil {
		http.Error(w, "could not load the room history", http.StatusInternalServerError)
		return
	}
	var buf bytes.Buffer
	if err := s.tpl.ExecuteTemplate(&buf, chatTemplateName, chatPage{
		Username: user.Username,
		MeID:     user.ID,
		Messages: toWire(rows),
	}); err != nil {
		http.Error(w, "could not render the chat page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// HandlePostMessage accepts JSON {body}, validates and trims it to
// 1..2000 bytes, stamps the authenticated author and time, persists the
// message under the room scope, and broadcasts it on the room topic. The
// publish is synchronous: the 201 reply is written only after the
// relay accepted the message, and it carries the same payload subscribers
// receive.
func (s *Service) HandlePostMessage(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.FromContext(r.Context())
	if !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	var req struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		auth.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	body := strings.TrimSpace(req.Body)
	switch {
	case body == "":
		auth.WriteError(w, http.StatusBadRequest, "message body is required")
		return
	case len(body) > maxBodyBytes:
		auth.WriteError(w, http.StatusBadRequest, fmt.Sprintf("message body must be at most %d bytes", maxBodyBytes))
		return
	}

	stored, err := s.store.AppendMessage(r.Context(), store.RoomScope, user.ID, user.Username, body, time.Now().Unix())
	if err != nil {
		auth.WriteError(w, http.StatusInternalServerError, "could not save the message")
		return
	}
	msg := Message{
		ID:         stored.ID,
		AuthorID:   stored.AuthorID,
		AuthorName: stored.AuthorName,
		Body:       stored.Body,
		CreatedAt:  stored.CreatedAt,
	}

	if err := s.pub.PublishJSON(store.RoomTopic, messageEvent, msg); err != nil {
		// Publish-failure policy: the row stays. The messages table is the
		// room's durable history, and every client heals a missed
		// broadcast by refetching /api/messages?after=<last seen id> when
		// its stream reconnects — deleting the row here would destroy an
		// accepted message that the backfill still delivers. The 502 tells
		// the sender only that the live broadcast degraded.
		auth.WriteError(w, http.StatusBadGateway, "message saved but not broadcast; it will appear on reconnect")
		return
	}
	auth.WriteJSON(w, http.StatusCreated, msg)
}

// HandleListMessages answers GET /api/messages?after=<id> with the room's
// most recent messages as a JSON array, oldest-first, capped at 50 rows.
// With after set, only messages with id strictly greater than after are
// returned (still the most recent 50 of them) — the shape reconnecting
// clients backfill with.
func (s *Service) HandleListMessages(w http.ResponseWriter, r *http.Request) {
	if _, ok := auth.FromContext(r.Context()); !ok {
		auth.WriteError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	after := int64(0)
	if raw := r.URL.Query().Get("after"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			auth.WriteError(w, http.StatusBadRequest, "after must be a non-negative message id")
			return
		}
		after = v
	}
	rows, err := s.store.RecentMessages(r.Context(), store.RoomScope, after, historyLimit)
	if err != nil {
		auth.WriteError(w, http.StatusInternalServerError, "could not load the room history")
		return
	}
	auth.WriteJSON(w, http.StatusOK, toWire(rows))
}

// toWire maps stored rows onto the wire shape; it keeps the result non-nil
// so JSON answers render as [] rather than null.
func toWire(rows []store.Message) []Message {
	messages := make([]Message, 0, len(rows))
	for _, m := range rows {
		messages = append(messages, Message{
			ID:         m.ID,
			AuthorID:   m.AuthorID,
			AuthorName: m.AuthorName,
			Body:       m.Body,
			CreatedAt:  m.CreatedAt,
		})
	}
	return messages
}
