// Command sample-im is a demonstration instant-messaging service built on the
// pushlet real-time push library. It runs pushlet in distributed mode over an
// embedded novaque relay backed by SQLite, so every publish is durably
// relayed through the database before reaching subscribers.
package main

import (
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
	"github.com/usual2970/sample-im/internal/presence"
	"github.com/usual2970/sample-im/internal/store"
)

// Default runtime settings. The listen address deliberately avoids the
// 9090/9091 pair used by the pushlet examples.
const (
	defaultAddr         = ":8080"
	defaultDBPath       = "data/relay.db"
	defaultAppDBPath    = "data/app.db"
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

// configFromEnv reads runtime configuration from the environment.
func configFromEnv() Config {
	return Config{
		Addr:      envOr("SAMPLE_IM_ADDR", defaultAddr),
		DBPath:    defaultDBPath,
		AppDBPath: defaultAppDBPath,
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
	cfg         Config
	db          *sql.DB
	appStore    *store.Store
	authSrv     *auth.Service
	chatSrv     *chat.Service
	presenceSrv *presence.Engine
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
		cfg.DBPath = defaultDBPath
	}
	if cfg.AppDBPath == "" {
		cfg.AppDBPath = defaultAppDBPath
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

	app := &App{cfg: cfg, db: db, appStore: appStore, authSrv: auth.NewService(appStore, tpl)}

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
	a.mux.HandleFunc("/api/register", a.authSrv.HandleRegister)
	a.mux.HandleFunc("/api/login", a.authSrv.HandleLogin)
	a.mux.Handle("/api/logout", a.authSrv.RequireAuth(http.HandlerFunc(a.authSrv.HandleLogout)))
	a.mux.Handle("/api/me", a.authSrv.RequireAuth(http.HandlerFunc(a.authSrv.HandleMe)))
	a.mux.HandleFunc("/login", a.authSrv.HandleLoginPage)
	a.mux.Handle("/chat", a.authSrv.RequireAuth(http.HandlerFunc(a.chatSrv.HandleChatPage)))
	a.mux.Handle("POST /api/messages", a.authSrv.RequireAuth(http.HandlerFunc(a.chatSrv.HandlePostMessage)))
	a.mux.Handle("GET /api/messages", a.authSrv.RequireAuth(http.HandlerFunc(a.chatSrv.HandleListMessages)))
	a.mux.Handle("POST /api/presence/join", a.authSrv.RequireAuth(http.HandlerFunc(a.presenceSrv.HandleJoin)))
	a.mux.Handle("POST /api/presence/heartbeat", a.authSrv.RequireAuth(http.HandlerFunc(a.presenceSrv.HandleHeartbeat)))
	a.mux.Handle("POST /api/leave", a.authSrv.RequireAuth(http.HandlerFunc(a.presenceSrv.HandleLeave)))
	a.mux.Handle("GET /static/", http.StripPrefix("/static/", staticHandler(staticRoot)))
	a.mux.HandleFunc("/", handleIndex)
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

// handleIndex answers with a short map of the service surface.
func handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "sample-im — pushlet demo")
	fmt.Fprintln(w, "  GET /health           liveness probe")
	fmt.Fprintln(w, "  GET /events?topic=t   SSE stream for topic t")
	fmt.Fprintln(w, "  GET /ws?topic=t       WebSocket stream for topic t")
	fmt.Fprintln(w, "  GET /login            sign in or create an account")
	fmt.Fprintln(w, "  GET /chat             the chat room page (session required)")
	fmt.Fprintln(w, "  POST /api/register    create an account (JSON)")
	fmt.Fprintln(w, "  POST /api/login       sign in (JSON)")
	fmt.Fprintln(w, "  POST /api/logout      sign out")
	fmt.Fprintln(w, "  GET /api/me           the signed-in user (JSON)")
	fmt.Fprintln(w, "  POST /api/messages    send a room message (JSON, session required)")
	fmt.Fprintln(w, "  GET /api/messages     recent room history (JSON, ?after=id)")
	fmt.Fprintln(w, "  POST /api/presence/join     announce yourself, get the online list")
	fmt.Fprintln(w, "  POST /api/presence/heartbeat  keep your online entry alive")
	fmt.Fprintln(w, "  POST /api/leave       leave the online list (sendBeacon-friendly)")
	fmt.Fprintln(w, "  GET /static/...       embedded client assets")
}

// dmTopicPrefix marks direct-message topics. Their names identify
// conversations between users and must never appear in logs.
const dmTopicPrefix = "dm:"

// redactedTopic replaces any dm: topic value in log output.
const redactedTopic = "dm:***"

// newRedactingLogger returns the [pushlet.Logger] factory used for the whole
// service. Everything passes through to the standard logger except that any
// "topic" field whose value starts with dm: is logged as dm:***, keeping
// conversation names out of logs.
func newRedactingLogger() pushlet.Logger {
	return &redactingLogger{fields: map[string]any{}}
}

// redactingLogger implements [pushlet.Logger] with dm: topic redaction.
// WithField copies the field set so scoped loggers stay independent, unlike
// the default logger which mutates its receiver.
type redactingLogger struct {
	fields map[string]any
}

// Println writes the redacted fields followed by v to the standard logger.
func (l *redactingLogger) Println(v ...any) {
	log.Println(append(l.prefix(), v...)...)
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
			if s, ok := v.(string); ok && strings.HasPrefix(s, dmTopicPrefix) {
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
