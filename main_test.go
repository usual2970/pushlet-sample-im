package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/usual2970/pushlet"
)

// testPollInterval keeps novaque relay round trips well inside the read
// deadlines used by the stream tests.
const testPollInterval = 25 * time.Millisecond

// newTestApp returns a started App served over an ephemeral port with its
// relay database inside t.TempDir(). The registered cleanups close the test
// server first and stop the app after (LIFO order), mirroring production
// shutdown.
func newTestApp(t *testing.T) (*App, *httptest.Server) {
	t.Helper()
	app, err := NewApp(Config{
		DBPath:       filepath.Join(t.TempDir(), "relay.db"),
		PollInterval: testPollInterval,
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	app.Start()
	t.Cleanup(app.Stop)
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)
	return app, ts
}

func TestHealthRoute(t *testing.T) {
	_, ts := newTestApp(t)

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("get /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /health body: %v", err)
	}
	if !strings.Contains(string(body), "ok") {
		t.Fatalf("body %q, want it to report ok", body)
	}
}

func TestIndexRoute(t *testing.T) {
	_, ts := newTestApp(t)

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
}

func TestSSEStreamSendsConnectedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, ts := newTestApp(t)

	stream := openSSE(t, ctx, ts.URL+"/events?topic=room")
	event, data := stream.next(t)
	if event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}
	if data == "" {
		t.Fatal("connected event carried empty data")
	}
}

// TestPublishJSONReachesSSESubscriberThroughRelay proves the novaque relay
// path end to end on SQLite: PublishJSON writes to the relay topic, the
// consumer polls it back, and the broker delivers to the subscribed client.
func TestPublishJSONReachesSSESubscriberThroughRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	app, ts := newTestApp(t)

	stream := openSSE(t, ctx, ts.URL+"/events?topic=room")
	if event, _ := stream.next(t); event != "connected" {
		t.Fatalf("first event %q, want connected", event)
	}

	sent := map[string]string{"from": "alice", "text": "hello room"}
	if err := app.push.PublishJSON("room", "chat", sent); err != nil {
		t.Fatalf("publish: %v", err)
	}

	for {
		event, data := stream.next(t)
		if event != "chat" {
			t.Logf("skipping event %q while waiting for chat", event)
			continue
		}
		var got map[string]string
		if err := json.Unmarshal([]byte(data), &got); err != nil {
			t.Fatalf("decode chat data %q: %v", data, err)
		}
		if got["from"] != sent["from"] || got["text"] != sent["text"] {
			t.Fatalf("got %v, want %v", got, sent)
		}
		return
	}
}

func TestWebSocketSubscriberReceivesPublishedMessage(t *testing.T) {
	app, ts := newTestApp(t)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?topic=room"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", wsURL, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	readMessage := func() pushlet.Message {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read websocket message: %v", err)
		}
		return parseWebSocketFrame(t, raw)
	}

	hello := readMessage()
	if hello.Event != "connected" {
		t.Fatalf("first websocket event %q, want connected", hello.Event)
	}

	sent := map[string]string{"from": "bob", "text": "hello ws"}
	if err := app.push.PublishJSON("room", "chat", sent); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg := readMessage()
	if msg.Event != "chat" {
		t.Fatalf("event %q, want chat", msg.Event)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(msg.Data), &got); err != nil {
		t.Fatalf("decode chat data %q: %v", msg.Data, err)
	}
	if got["text"] != sent["text"] {
		t.Fatalf("got %v, want text %q", got, sent["text"])
	}
}

// TestPushHandlersUnavailableBeforeStart pins the ordering constraint:
// handlers mounted by NewApp answer 503 until App.Start runs the broker, so
// Start must be called before serving traffic.
func TestPushHandlersUnavailableBeforeStart(t *testing.T) {
	app, err := NewApp(Config{
		DBPath:       filepath.Join(t.TempDir(), "relay.db"),
		PollInterval: testPollInterval,
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(app.Stop)
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events?topic=room", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get /events before start: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
}

func TestRedactingLoggerRedactsDMTopics(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	newRedactingLogger().
		WithField("client_id", "c1").
		WithField("topic", "dm:alice/bob").
		Println("New client requested connection")

	out := buf.String()
	if strings.Contains(out, "alice") {
		t.Fatalf("dm topic leaked to log: %q", out)
	}
	if !strings.Contains(out, redactedTopic) {
		t.Fatalf("log %q missing redaction marker %q", out, redactedTopic)
	}
	if !strings.Contains(out, "New client requested connection") {
		t.Fatalf("log %q dropped the message", out)
	}

	// Non-dm topics pass through, and WithField leaves the parent untouched.
	buf.Reset()
	base := newRedactingLogger().WithField("topic", "room")
	base.WithField("client_id", "c2").Println("scoped")
	if !strings.Contains(buf.String(), "room") || !strings.Contains(buf.String(), "c2") {
		t.Fatalf("log %q lost pass-through fields", buf.String())
	}
	buf.Reset()
	base.Println("base")
	if strings.Contains(buf.String(), "c2") {
		t.Fatalf("WithField mutated the parent logger: %q", buf.String())
	}
}

// sseStream reads one Server-Sent Events connection into parsed events.
// All reads are bound by the context passed to openSSE.
type sseStream struct {
	body  io.ReadCloser
	lines *bufio.Scanner
}

// openSSE subscribes to urlStr and fails the test unless the endpoint
// answers 200 with a text/event-stream body.
func openSSE(t *testing.T, ctx context.Context, urlStr string) *sseStream {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", urlStr, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get %s: status %d, want 200", urlStr, resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("get %s: content type %q, want text/event-stream", urlStr, ct)
	}
	return &sseStream{body: resp.Body, lines: bufio.NewScanner(resp.Body)}
}

// next returns the next event/data pair on the stream, skipping heartbeat
// comments. It fails the test when the stream ends or errors before a full
// event arrives.
func (s *sseStream) next(t *testing.T) (event, data string) {
	t.Helper()
	got := false
	for s.lines.Scan() {
		line := s.lines.Text()
		switch {
		case strings.HasPrefix(line, ":"):
			// Heartbeat comment; ignore.
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
			got = true
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
			got = true
		case line == "":
			if got {
				return event, data
			}
		}
	}
	if err := s.lines.Err(); err != nil {
		t.Fatalf("sse read: %v", err)
	}
	t.Fatal("sse stream ended before a full event arrived")
	return "", ""
}

// parseWebSocketFrame decodes one pushlet WebSocket frame: the connected
// welcome is bare JSON, published frames are "<topic> <json>".
func parseWebSocketFrame(t *testing.T, raw []byte) pushlet.Message {
	t.Helper()
	payload := string(raw)
	if !strings.HasPrefix(payload, "{") {
		if i := strings.IndexByte(payload, ' '); i >= 0 {
			payload = payload[i+1:]
		}
	}
	var msg pushlet.Message
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		t.Fatalf("decode websocket frame %q: %v", raw, err)
	}
	return msg
}
