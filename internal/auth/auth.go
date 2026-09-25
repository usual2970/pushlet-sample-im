// Package auth implements sample-im's account system: username+password
// registration with pair-uniqueness, bcrypt login, cookie-backed sessions,
// and the server-rendered combined login/registration page.
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/usual2970/sample-im/internal/store"
)

// SessionCookieName is the cookie carrying the session token.
const SessionCookieName = "sample_im_session"

// sessionMaxAge bounds how long a login session stays valid.
const sessionMaxAge = 30 * 24 * time.Hour

// loginTemplateName is the single embedded page served at /login.
const loginTemplateName = "login.html"

// contextKey is an unexported type so context values set by this package
// cannot collide with values from other packages.
type contextKey struct{}

// userContextKey holds the resolved *store.User under [Service.RequireAuth].
var userContextKey contextKey

// ErrWrongCredentials reports a login attempt that matched no account; the
// message deliberately does not say whether the username or the password was
// wrong.
var ErrWrongCredentials = errors.New("wrong username or password")

// Service serves the auth HTTP surface over a [store.Store]: the JSON API
// (register, login, logout, me), the /login page, and the session middleware.
type Service struct {
	store *store.Store
	tpl   *template.Template
}

// NewService returns a Service persisting accounts in st and rendering the
// login page with tpl (parsed from login.html by the caller).
func NewService(st *store.Store, tpl *template.Template) *Service {
	return &Service{store: st, tpl: tpl}
}

// FromContext returns the user resolved by [Service.RequireAuth], if any.
// Later units (chat, presence, DMs) use this to identify the requester.
func FromContext(ctx context.Context) (*store.User, bool) {
	user, ok := ctx.Value(userContextKey).(*store.User)
	return user, ok && user != nil
}

// RequireAuth wraps next so only requests carrying a valid session cookie
// reach it. The resolved user is stored in the request context (see
// [FromContext]). Unauthenticated requests are rejected the way their caller
// expects: JSON 401 for /api/ paths, a redirect to /login for pages.
func (s *Service) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := s.resolveUser(r)
		if user == nil {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				writeError(w, http.StatusUnauthorized, "authentication required")
				return
			}
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userContextKey, user)))
	})
}

// HandleLoginPage serves the combined login/registration page. Visitors that
// are already signed in are sent straight to /chat.
func (s *Service) HandleLoginPage(w http.ResponseWriter, r *http.Request) {
	if s.resolveUser(r) != nil {
		http.Redirect(w, r, "/chat", http.StatusFound)
		return
	}
	var buf bytes.Buffer
	if err := s.tpl.ExecuteTemplate(&buf, loginTemplateName, nil); err != nil {
		writeError(w, http.StatusInternalServerError, "could not render the login page")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(buf.Bytes())
}

// HandleRegister creates an account from JSON {username, password}, logs it
// in (new session + cookie), and returns the user JSON. A (username,
// password) pair that already exists is rejected with 409; validation
// failures with 400.
func (s *Service) HandleRegister(w http.ResponseWriter, r *http.Request) {
	creds, ok := decodeCredentials(w, r)
	if !ok {
		return
	}
	if err := validateCredentials(creds); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	user, err := s.store.Register(r.Context(), creds.Username, []byte(creds.Password))
	switch {
	case errors.Is(err, store.ErrPairExists):
		writeError(w, http.StatusConflict, store.ErrPairExists.Error())
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "could not create the account")
		return
	}

	if err := s.startSession(w, r, user.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "could not start a session")
		return
	}
	writeUser(w, user)
}

// HandleLogin signs in from JSON {username, password}. Several accounts may
// share a username; the first one whose bcrypt hash matches the password
// wins. Credentials that match nothing answer 401.
func (s *Service) HandleLogin(w http.ResponseWriter, r *http.Request) {
	creds, ok := decodeCredentials(w, r)
	if !ok {
		return
	}
	if err := validateCredentials(creds); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	users, err := s.store.UsersByName(r.Context(), creds.Username)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not look up the account")
		return
	}
	var matched *store.User
	for i := range users {
		if bcrypt.CompareHashAndPassword([]byte(users[i].PasswordHash), []byte(creds.Password)) == nil {
			matched = &users[i]
			break
		}
	}
	if matched == nil {
		writeError(w, http.StatusUnauthorized, ErrWrongCredentials.Error())
		return
	}

	if err := s.startSession(w, r, matched.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "could not start a session")
		return
	}
	writeUser(w, matched)
}

// HandleLogout deletes the current session row and clears the cookie. It is
// mounted behind [Service.RequireAuth], so it always runs with a session.
func (s *Service) HandleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(SessionCookieName); err == nil {
		if err := s.store.DeleteSession(r.Context(), c.Value); err != nil {
			writeError(w, http.StatusInternalServerError, "could not delete the session")
			return
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// HandleMe describes the signed-in user. U5 will extend the response with
// the user's direct-message topic.
func (s *Service) HandleMe(w http.ResponseWriter, r *http.Request) {
	user, ok := FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	writeUser(w, user)
}

// resolveUser maps a request's session cookie to its account, returning nil
// for anonymous requests.
func (s *Service) resolveUser(r *http.Request) *store.User {
	c, err := r.Cookie(SessionCookieName)
	if err != nil {
		return nil
	}
	user, err := s.store.UserBySession(r.Context(), c.Value)
	if err != nil {
		return nil
	}
	return user
}

// startSession mints a session token, persists it for userID, and sets the
// session cookie.
func (s *Service) startSession(w http.ResponseWriter, r *http.Request, userID string) error {
	token, err := s.store.NewSessionToken()
	if err != nil {
		return fmt.Errorf("mint token: %w", err)
	}
	if err := s.store.CreateSession(r.Context(), token, userID); err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionMaxAge.Seconds()),
	})
	return nil
}

// credentials is the request body of /api/register and /api/login.
type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// decodeCredentials reads the JSON body and trims the username (R1: a name
// with padded whitespace registers under its trimmed form). On failure it has
// already written the 400 response and reports false.
func decodeCredentials(w http.ResponseWriter, r *http.Request) (credentials, bool) {
	var creds credentials
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&creds); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return credentials{}, false
	}
	creds.Username = strings.TrimSpace(creds.Username)
	return creds, true
}

// maxPasswordBytes is bcrypt's hard input limit at the pinned x/crypto: the
// algorithm ignores bytes past 72, so anything longer is rejected outright
// rather than silently truncated.
const maxPasswordBytes = 72

// validateCredentials enforces the registration rules R1/R2 on a decoded
// request: the username is trimmed and must be 1-32 characters of
// [A-Za-z0-9_-]; the password must be 1-72 bytes. Login uses the same rules,
// since accounts can only exist within them.
func validateCredentials(creds credentials) error {
	username := strings.TrimSpace(creds.Username)
	switch {
	case username == "":
		return errors.New("username is required")
	case len(username) > 32:
		return errors.New("username must be at most 32 characters")
	case !validUsername(username):
		return errors.New("username may only contain letters, digits, '-' and '_'")
	}
	switch n := len(creds.Password); {
	case n == 0:
		return errors.New("password is required")
	case n > maxPasswordBytes:
		return fmt.Errorf("password must be at most %d bytes", maxPasswordBytes)
	}
	return nil
}

// validUsername reports whether every byte of s is in [A-Za-z0-9_-].
func validUsername(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

// writeUser answers with the JSON shape returned by register, login, and me.
func writeUser(w http.ResponseWriter, user *store.User) {
	writeJSON(w, http.StatusOK, map[string]string{
		"id":       user.ID,
		"username": user.Username,
	})
}

// writeJSON writes v as a JSON response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes {"error": message} with the given status.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
