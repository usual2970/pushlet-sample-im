package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/usual2970/sample-im/internal/auth"
	"github.com/usual2970/sample-im/internal/store"
)

// meReply is the decoded GET /api/me answer.
type meReply struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	DMTopic  string `json:"dm_topic"`
}

// fetchMe GETs /api/me carrying the session cookie and decodes the reply.
func fetchMe(t *testing.T, tsURL, session string) meReply {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, tsURL+"/api/me", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get /api/me: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/api/me status %d, want 200", resp.StatusCode)
	}
	var me meReply
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		t.Fatalf("decode /api/me: %v", err)
	}
	return me
}

// dmPost sends {to, body} to /api/dm carrying the session cookie and returns
// the raw response for the caller to decode.
func dmPost(t *testing.T, tsURL, session, to, body string) *http.Response {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"to": to, "body": body})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, tsURL+"/api/dm", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post /api/dm: %v", err)
	}
	return resp
}

// dmHistoryWith GETs /api/dm?with=<id> and returns the status and raw body.
func dmHistoryWith(t *testing.T, tsURL, session, with string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, tsURL+"/api/dm?with="+with, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get /api/dm: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// TestDMEndToEndThroughRelay drives the full U5 chain across every layer with
// no mocks: register bob+carol+dave over HTTP, join bob & carol into presence,
// subscribe SSE clients to all three users' private dm topics plus the room,
// and watch bob's DM reach carol's topic and his own copy — and only those.
// Sentinels published strictly after the DM prove the room topic and dave's
// topic stayed silent: anything that leaked would have been published (and
// per-topic relay order, delivered) before the sentinel.
func TestDMEndToEndThroughRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, ts := newTestApp(t)

	// Anonymous DM calls are rejected like every other /api route.
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		req, err := http.NewRequest(method, ts.URL+"/api/dm", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("anonymous %s /api/dm: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("anonymous %s /api/dm status %d, want 401", method, resp.StatusCode)
		}
	}

	bobCookie, bobID := registerViaAPI(t, ts, "bob", "pw-bob")
	carolCookie, carolID := registerViaAPI(t, ts, "carol", "pw-carol")
	daveCookie, daveID := registerViaAPI(t, ts, "dave", "pw-dave")

	// /api/me hands each user their own private dm topic (KTD6): dm:-prefixed
	// and built on a secret of at least 22 chars.
	bobMe := fetchMe(t, ts.URL, bobCookie)
	carolMe := fetchMe(t, ts.URL, carolCookie)
	daveMe := fetchMe(t, ts.URL, daveCookie)
	for name, me := range map[string]meReply{"bob": bobMe, "carol": carolMe, "dave": daveMe} {
		if me.ID == "" || me.Username != name {
			t.Fatalf("/api/me for %s returned %+v", name, me)
		}
		if !strings.HasPrefix(me.DMTopic, store.DMTopicPrefix) {
			t.Fatalf("%s's dm_topic %q does not start with %q", name, me.DMTopic, store.DMTopicPrefix)
		}
		if len(me.DMTopic) < len(store.DMTopicPrefix)+22 {
			t.Fatalf("%s's dm_topic %q is shorter than prefix + 22 chars", name, me.DMTopic)
		}
	}

	// Only the sender's target needs presence; dave stays entirely offline.
	joinPresence(t, ts, bobCookie)
	joinPresence(t, ts, carolCookie)

	// The room stream is subscribed after the joins so its only expected
	// traffic post-DM is the sentinel; presence snapshots still arrive and
	// are skipped explicitly.
	carolStream := openSSE(t, ctx, ts.URL+"/events?topic="+carolMe.DMTopic)
	bobStream := openSSE(t, ctx, ts.URL+"/events?topic="+bobMe.DMTopic)
	daveStream := openSSE(t, ctx, ts.URL+"/events?topic="+daveMe.DMTopic)
	roomStream := openSSE(t, ctx, ts.URL+"/events?topic=room")
	for _, s := range []*sseStream{carolStream, bobStream, daveStream, roomStream} {
		if event, _ := s.next(t); event != "connected" {
			t.Fatalf("first event %q, want connected", event)
		}
	}

	postResp := dmPost(t, ts.URL, bobCookie, carolID, "psst carol 🎉")
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		t.Fatalf("bob's DM status %d (body %q), want 201", postResp.StatusCode, body)
	}
	var reply struct {
		ID         int64  `json:"id"`
		Scope      string `json:"scope"`
		AuthorID   string `json:"author_id"`
		AuthorName string `json:"author_name"`
		To         string `json:"to"`
		Body       string `json:"body"`
		CreatedAt  int64  `json:"created_at"`
	}
	if err := json.NewDecoder(postResp.Body).Decode(&reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.ID == 0 || reply.AuthorID != bobID || reply.AuthorName != "bob" ||
		reply.To != carolID || reply.Body != "psst carol 🎉" || reply.CreatedAt == 0 {
		t.Fatalf("reply %+v is not the server-stamped DM", reply)
	}
	wantScope := store.DMTopicPrefix
	if bobID > carolID {
		wantScope += carolID + ":" + bobID
	} else {
		wantScope += bobID + ":" + carolID
	}
	if reply.Scope != wantScope {
		t.Fatalf("reply scope %q, want %q", reply.Scope, wantScope)
	}

	// Carol's private stream receives the DM; bob's receives his own copy.
	for name, s := range map[string]*sseStream{"carol": carolStream, "bob (own copy)": bobStream} {
		for {
			event, data := s.next(t)
			if event != "message" {
				t.Logf("skipping event %q on %s's stream", event, name)
				continue
			}
			var got struct {
				ID         int64  `json:"id"`
				Scope      string `json:"scope"`
				AuthorID   string `json:"author_id"`
				AuthorName string `json:"author_name"`
				To         string `json:"to"`
				Body       string `json:"body"`
				CreatedAt  int64  `json:"created_at"`
			}
			if err := json.Unmarshal([]byte(data), &got); err != nil {
				t.Fatalf("decode %s's dm data %q: %v", name, data, err)
			}
			if got != reply {
				t.Fatalf("%s's dm stream got %+v, want %+v", name, got, reply)
			}
			break
		}
	}

	// The room topic and dave's dm topic must have stayed silent: publish a
	// sentinel to each and require it to be the next eligible event.
	if err := app.push.PublishJSON("room", "sentinel", map[string]string{"k": "room"}); err != nil {
		t.Fatalf("publish room sentinel: %v", err)
	}
	for {
		event, _ := roomStream.next(t)
		if event == "presence" {
			continue // joins/sweeps republish snapshots; expected noise
		}
		if event != "sentinel" {
			t.Fatalf("room stream saw %q before the sentinel; the DM leaked to the room", event)
		}
		break
	}
	if err := app.push.PublishJSON(daveMe.DMTopic, "sentinel", map[string]string{"k": "dave"}); err != nil {
		t.Fatalf("publish dave sentinel: %v", err)
	}
	if event, _ := daveStream.next(t); event != "sentinel" {
		t.Fatalf("dave's dm stream saw %q before the sentinel; carol's DM leaked to dave", event)
	}

	// History: carol reads her exchange with bob; dave's caller-relative
	// query for bob stays empty (the third-user authorization).
	status, raw := dmHistoryWith(t, ts.URL, carolCookie, bobID)
	if status != http.StatusOK {
		t.Fatalf("carol's history status %d, want 200", status)
	}
	if !strings.Contains(raw, "psst carol") {
		t.Fatalf("carol's history %q lost the DM", raw)
	}
	status, raw = dmHistoryWith(t, ts.URL, daveCookie, bobID)
	if status != http.StatusOK {
		t.Fatalf("dave's history status %d, want 200", status)
	}
	if strings.TrimSpace(raw) != "[]" {
		t.Fatalf("dave's with=bob history %q, want an empty array — never carol's messages", raw)
	}

	// Sending through the app wiring hits the presence gate: dave never
	// joined, so bob→dave is a 409, and an unknown id is a 404.
	offline := dmPost(t, ts.URL, bobCookie, daveID, "you there?")
	defer offline.Body.Close()
	if offline.StatusCode != http.StatusConflict {
		t.Fatalf("DM to offline dave status %d, want 409", offline.StatusCode)
	}
	unknown := dmPost(t, ts.URL, bobCookie, "no-such-user", "hello?")
	defer unknown.Body.Close()
	if unknown.StatusCode != http.StatusNotFound {
		t.Fatalf("DM to unknown user status %d, want 404", unknown.StatusCode)
	}
}
