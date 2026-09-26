// Command sample-im is a demonstration instant-messaging service built on the
// pushlet real-time push library. It runs pushlet in distributed mode over an
// embedded novaque relay backed by SQLite, so every publish is durably
// relayed through the database before reaching subscribers.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/usual2970/novaque"
	"github.com/usual2970/novaque/driver/sqlite"
	"github.com/usual2970/pushlet"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/chat"
	"github.com/usual2970/sample-im/internal/dm"
	"github.com/usual2970/sample-im/internal/presence"
	"github.com/usual2970/sample-im/internal/store"
)

// Default runtime settings. The listen address deliberately avoids the
// 9090/9091 pair used by the pushlet examples.
const (
	defaultAddr         = ":8080"
	defaultDataDir      = "data"
	defaultShutdownWait = 5 * time.Second
)

// templateFS embeds the server-rendered pages so the single binary serves
// them no matter where it runs from.
//
//go:embed web/templates/*.html
var templateFS embed.FS

// staticFS embeds the client assets (stylesheet, room script) served under
// /static/.
//
//go:embed web/static
var staticFS embed.FS

// Config controls one sample-im process. Zero-valued fields fall back to the
// package defaults above; tests override the database path and relay poll
// interval to stay fast and hermetic.
type Config struct {
	// Addr is the HTTP listen address, served directly by main. Tests serve
	// Handler through httptest instead and leave Addr empty.
	Addr string

	// DBPath is the SQLite file backing the novaque relay.
	DBPath string

	// AppDBPath is the SQLite file backing the application store
	// (accounts, sessions, messages). It is kept separate from the relay
	// database.
	AppDBPath string

	// PollInterval is the novaque consumer idle poll for the relay topic.
	// Zero keeps novaque's default (200ms with jitter); tests lower it to
	// make relay round trips land well inside read deadlines.
	PollInterval time.Duration
}

// configFromEnv reads runtime configuration from the environment. The data
// directory is the single knob for both SQLite files: the novaque relay
// database and the application database live side by side under it, so
// pointing SAMPLE_IM_DATA_DIR at a fresh directory is all a second demo
// instance needs.
func configFromEnv() Config {
	dataDir := envOr("SAMPLE_IM_DATA_DIR", defaultDataDir)
	return Config{
		Addr:      envOr("SAMPLE_IM_ADDR", defaultAddr),
		DBPath:    filepath.Join(dataDir, "relay.db"),
		AppDBPath: filepath.Join(dataDir, "app.db"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// App wires the SQLite-backed novaque relay, the pushlet instance, the
// application store, and the HTTP handlers for one process. Construct it
// with [NewApp], serve [App.Handler], and always pair [App.Start] with
// [App.Stop].
type App struct {
	db          *sql.DB
	appStore    *store.Store
	authSrv     *auth.Service
	chatSrv     *chat.Service
	presenceSrv *presence.Engine
	dmSrv       *dm.Service
	push        *pushlet.Pushlet
	mux         *http.ServeMux
	stopOn      sync.Once
}

// NewApp opens the relay and application databases, constructs the pushlet
// instance in distributed mode, and mounts its handlers. Nothing runs until
// [App.Start]; before that, the push handlers answer 503 because the broker
// is not yet running. EnableDistributedNovaque also migrates the novaque
// schema, so a broken database fails here rather than at first publish.
func NewApp(cfg Config) (*App, error) {
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(defaultDataDir, "relay.db")
	}
	if cfg.AppDBPath == "" {
		cfg.AppDBPath = filepath.Join(defaultDataDir, "app.db")
	}

	db, err := openRelayDB(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	appStore, err := store.Open(cfg.AppDBPath)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	closeDBs := func() {
		_ = db.Close()
		_ = appStore.Close()
	}

	tpl, err := template.ParseFS(templateFS, "web/templates/*.html")
	if err != nil {
		closeDBs()
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	staticRoot, err := fs.Sub(staticFS, "web/static")
	if err != nil {
		closeDBs()
		return nil, fmt.Errorf("static assets: %w", err)
	}

	app := &App{db: db, appStore: appStore, authSrv: auth.NewService(appStore, tpl)}

	opts := pushlet.DefaultDistributedOptions()
	if cfg.PollInterval > 0 {
		opts.Novaque.PollInterval = cfg.PollInterval
	}
	client, err := novaque.Open(sqlite.New(db), opts.Novaque)
	if err != nil {
		closeDBs()
		return nil, fmt.Errorf("novaque open: %w", err)
	}

	app.push = pushlet.New(pushlet.WithLogger(newRedactingLogger))
	if err := app.push.EnableDistributedNovaque(client, opts); err != nil {
		closeDBs()
		return nil, fmt.Errorf("distributed mode: %w", err)
	}
	app.chatSrv = chat.NewService(appStore, app.push, tpl)
	app.presenceSrv = presence.NewEngine(app.push, presence.Config{})
	app.dmSrv = dm.NewService(appStore, app.push, app.presenceSrv)

	app.mux = http.NewServeMux()
	app.mountRoutes(staticRoot)
	return app, nil
}

// mountRoutes wires the HTTP surface. Keep this in one place so later units
// (presence, DMs) extend the service by mounting more handlers here.
// staticRoot is the embedded client-asset tree served under /static/.
func (a *App) mountRoutes(staticRoot fs.FS) {
	a.mux.HandleFunc("/health", handleHealth)
	a.mux.HandleFunc("/events", a.push.HandleSSE)
	a.mux.HandleFunc("/ws", a.push.HandleWebsocket)
	a.mux.HandleFunc("/login", a.authSrv.HandleLoginPage)
	a.mux.Handle("/chat", a.authSrv.RequireAuth(http.HandlerFunc(a.chatSrv.HandleChatPage)))
	a.mux.Handle("GET /static/", http.StripPrefix("/static/", staticHandler(staticRoot)))
	// The site root IS the chat page; unauthenticated visitors are
	// redirected to /login by the auth middleware, same as /chat.
	a.mux.Handle("/", a.authSrv.RequireAuth(http.HandlerFunc(a.chatSrv.HandleChatPage)))

	// The /api/* subtree lives on its own mux where every route carries a
	// method pattern: ServeMux then answers a request whose path matches a
	// route but whose verb does not with a built-in 405 plus Allow header,
	// so a cross-site top-level GET to /api/logout can no longer delete the
	// session (logout CSRF under SameSite=Lax cookies). The subtree must be
	// its own mux for that: on the main mux the "/" index pattern would
	// swallow every unmatched /api request. Both built-ins are text/plain,
	// which would break the JSON envelope every other /api answer uses, so
	// apiJSONErrors re-renders them.
	api := http.NewServeMux()
	api.HandleFunc("POST /api/register", a.authSrv.HandleRegister)
	api.HandleFunc("POST /api/login", a.authSrv.HandleLogin)
	api.Handle("POST /api/logout", a.authSrv.RequireAuth(http.HandlerFunc(a.authSrv.HandleLogout)))
	api.Handle("GET /api/me", a.authSrv.RequireAuth(http.HandlerFunc(a.authSrv.HandleMe)))
	api.Handle("POST /api/messages", a.authSrv.RequireAuth(http.HandlerFunc(a.chatSrv.HandlePostMessage)))
	api.Handle("GET /api/messages", a.authSrv.RequireAuth(http.HandlerFunc(a.chatSrv.HandleListMessages)))
	api.Handle("POST /api/presence/join", a.authSrv.RequireAuth(http.HandlerFunc(a.presenceSrv.HandleJoin)))
	api.Handle("POST /api/presence/heartbeat", a.authSrv.RequireAuth(http.HandlerFunc(a.presenceSrv.HandleHeartbeat)))
	api.Handle("POST /api/presence/leave", a.authSrv.RequireAuth(http.HandlerFunc(a.presenceSrv.HandleLeave)))
	api.Handle("POST /api/dm", a.authSrv.RequireAuth(http.HandlerFunc(a.dmSrv.HandleSend)))
	api.Handle("GET /api/dm", a.authSrv.RequireAuth(http.HandlerFunc(a.dmSrv.HandleHistory)))
	api.Handle("GET /api/users/{id}", a.authSrv.RequireAuth(http.HandlerFunc(a.dmSrv.HandleUser)))
	a.mux.Handle("/api/", apiJSONErrors(api))
}

// apiJSONErrors wraps the /api/* route mux so its built-in 404 and 405
// answers render in the {"error":...} envelope every other /api handler
// uses, instead of ServeMux's text/plain bodies. The status and the Allow
// header the mux sets on 405s pass through unchanged.
func apiJSONErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &bufferedResponse{header: w.Header()}
		next.ServeHTTP(rec, r)
		switch rec.status {
		case http.StatusNotFound:
			auth.WriteError(w, http.StatusNotFound, "no such endpoint")
		case http.StatusMethodNotAllowed:
			auth.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		default:
			rec.replay(w)
		}
	})
}

// bufferedResponse records one response's status code and body without
// touching the client connection, so a 404/405 produced by the wrapped mux
// can be re-rendered as JSON after the fact. Every /api response is a small
// JSON document — nothing under /api streams — so buffering them costs
// nothing. A handler that writes nothing leaves the status zero and the body
// empty, which replay treats as "let Go write its own empty 200".
type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

// Header returns the wrapped response's header map, so headers the wrapped
// handler sets (Content-Type, Allow, Set-Cookie) land on the real response.
func (b *bufferedResponse) Header() http.Header { return b.header }

// WriteHeader records the status instead of sending it; only the first one
// counts, mirroring net/http's own rule.
func (b *bufferedResponse) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}

// Write records the body, implying a 200 status when none was written.
func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.body.Write(p)
}

// replay sends the recorded status and body to w.
func (b *bufferedResponse) replay(w http.ResponseWriter) {
	if b.status == 0 && b.body.Len() == 0 {
		return
	}
	if b.status != 0 {
		w.WriteHeader(b.status)
	}
	if b.body.Len() > 0 {
		_, _ = w.Write(b.body.Bytes())
	}
}

// staticHandler serves the embedded client assets. Responses carry
// Cache-Control: no-cache so demo deployments pick up asset changes on
// refresh instead of serving a stale script.
func staticHandler(root fs.FS) http.Handler {
	fileServer := http.FileServerFS(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})
}

// Start runs the pushlet broker, the relay fan-out goroutines, and the
// presence sweeper. It must be called after [NewApp] and before serving
// traffic; call it exactly once.
func (a *App) Start() {
	a.push.Start()
	a.presenceSrv.Start()
}

// Stop shuts the presence sweeper, the pushlet instance (broker, relay
// consumer, novaque client, in that order), and then closes both databases.
// It is safe to call more than once. Servers serving [App.Handler] must be
// shut down before Stop so live streams drain first.
func (a *App) Stop() {
	a.stopOn.Do(func() {
		a.presenceSrv.Stop()
		a.push.Stop()
		_ = a.db.Close()
		_ = a.appStore.Close()
	})
}

// Handler returns the HTTP handler serving the whole service.
func (a *App) Handler() http.Handler {
	return a.mux
}

// openRelayDB opens the SQLite database at path with the pragmas novaque
// needs (foreign keys, WAL, 10s busy timeout) and verifies it is reachable.
func openRelayDB(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	dsn := "file:" + path + "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(10)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping: %w", err)
	}
	return db, nil
}

// handleHealth answers 200 while the process is serving, for liveness probes.
func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"status":"ok"}`)
}

// redactedTopic replaces any dm: topic value in log output.
const redactedTopic = "dm:***"

// newRedactingLogger returns the [pushlet.Logger] factory used for the whole
// service. Everything passes through to the standard logger except that dm:
// topics never reach it: any "topic" field whose value starts with dm: is
// logged as dm:***, and any raw message operand embedding a dm: topic —
// pushlet's WebSocket read path logs received command lines like
// "SUB dm:<secret>" as operands, not fields — is truncated at the prefix the
// same way, keeping conversation names out of logs.
func newRedactingLogger() pushlet.Logger {
	return &redactingLogger{fields: map[string]any{}}
}

// redactingLogger implements [pushlet.Logger] with dm: topic redaction.
// WithField copies the field set so scoped loggers stay independent, unlike
// the default logger which mutates its receiver.
type redactingLogger struct {
	fields map[string]any
}

// Println writes the redacted fields followed by v to the standard logger,
// scrubbing dm: topics out of the message operands as well as the fields.
func (l *redactingLogger) Println(v ...any) {
	log.Println(append(l.prefix(), redactOperands(v)...)...)
}

// redactOperands returns v with every string or []byte operand that embeds a
// dm: topic rewritten to stop at the prefix: "SUB dm:<secret>" becomes
// "SUB dm:***". Everything from the prefix onward was topic material, so all
// of it goes. The input slice is never mutated; a copy is made only when
// some operand actually needs redacting.
func redactOperands(v []any) []any {
	var out []any // nil until the first redaction forces a copy
	for i, op := range v {
		var redacted string
		switch s := op.(type) {
		case string:
			if j := strings.Index(s, store.DMTopicPrefix); j >= 0 {
				redacted = s[:j] + redactedTopic
			}
		case []byte:
			if j := bytes.Index(s, []byte(store.DMTopicPrefix)); j >= 0 {
				redacted = string(s[:j]) + redactedTopic
			}
		default:
			continue
		}
		if redacted == "" {
			continue // operand carried no dm: topic
		}
		if out == nil {
			out = slices.Clone(v)
		}
		out[i] = redacted
	}
	if out == nil {
		return v
	}
	return out
}

// WithField returns a new logger with key set to value, leaving the
// receiver untouched.
func (l *redactingLogger) WithField(key string, value any) pushlet.Logger {
	next := &redactingLogger{fields: make(map[string]any, len(l.fields)+1)}
	maps.Copy(next.fields, l.fields)
	next.fields[key] = value
	return next
}

// prefix renders the field set as sorted "key: value" pairs, redacting dm:
// topic values.
func (l *redactingLogger) prefix() []any {
	out := make([]any, 0, len(l.fields))
	for _, k := range slices.Sorted(maps.Keys(l.fields)) {
		v := l.fields[k]
		if k == "topic" {
			if s, ok := v.(string); ok && strings.HasPrefix(s, store.DMTopicPrefix) {
				v = redactedTopic
			}
		}
		out = append(out, fmt.Sprintf("%s: %v", k, v))
	}
	return out
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run boots the service and blocks until SIGINT/SIGTERM, then shuts down in
// the order pushlet requires: HTTP server first (draining live streams),
// then the pushlet instance (broker, relay consumer, novaque client), then
// the relay database.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := configFromEnv()
	app, err := NewApp(cfg)
	if err != nil {
		return err
	}

	app.Start()

	srv := &http.Server{Addr: cfg.Addr, Handler: app.Handler()}
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("sample-im listening on %s (SSE /events, WebSocket /ws, health /health)", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		_ = srv.Close()
		app.Stop()
		return err
	case <-ctx.Done():
	}

	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownWait)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	app.Stop()
	return nil
}
