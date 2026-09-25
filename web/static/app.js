'use strict';

// sample-im room + DM client. Subscribes to the fixed "room" SSE topic and,
// after /api/me reveals the caller's private dm topic, to a second stream for
// their direct messages. Merges stream messages with server-rendered history
// keyed on server message ids, posts room messages to /api/messages and DMs
// to /api/dm, and tracks the caller's presence (join on every (re)connect,
// heartbeats, a leave beacon on unload).
// Security rule (KTD12): every piece of dynamic text is set through
// textContent — user content never touches innerHTML.

const messagesList = document.getElementById('messages');
const composerForm = document.getElementById('composer');
const composerInput = document.getElementById('composer-input');
const composerError = document.getElementById('composer-error');
const connBanner = document.getElementById('conn-banner');
const logoutButton = document.getElementById('logout');
const onlineList = document.getElementById('online-list');

const dmMessages = document.getElementById('dm-messages');
const dmTitle = document.getElementById('dm-title');
const dmComposerForm = document.getElementById('dm-composer');
const dmInput = document.getElementById('dm-input');
const dmSend = document.getElementById('dm-send');
const dmError = document.getElementById('dm-error');
const dmList = document.getElementById('dm-list');

let lastSeenId = 0;         // highest server message id rendered
const seenIds = new Set();  // every server message id rendered

// ---- rendering ----

function formatTime(unixSeconds) {
  const d = new Date(unixSeconds * 1000);
  const pad = (n) => String(n).padStart(2, '0');
  return pad(d.getHours()) + ':' + pad(d.getMinutes());
}

function messageRow(msg) {
  const row = document.createElement('li');
  row.className = 'msg';
  row.dataset.id = String(msg.id);

  const meta = document.createElement('span');
  meta.className = 'meta';

  const author = document.createElement('span');
  author.className = 'author';
  author.textContent = msg.author_name;

  const time = document.createElement('time');
  time.textContent = formatTime(msg.created_at);

  meta.append(author, time);

  const body = document.createElement('span');
  body.className = 'body';
  body.textContent = msg.body;

  row.append(meta, body);
  return row;
}

function nearBottom(list) {
  return list.scrollHeight - list.scrollTop - list.clientHeight < 120;
}

// mergeInto renders msg in list unless its id is already there, inserting it
// in id order: drop any .empty placeholder, remember whether the list was
// scrolled near the bottom, place the row before the first sibling carrying a
// higher id (append when there is none), track the id in seen, and autoscroll
// when the list was stuck to the bottom. isOwn flags the caller's own
// messages for right alignment.
function mergeInto(list, seen, msg, isOwn) {
  if (!msg || !Number.isInteger(msg.id) || seen.has(msg.id)) return;

  const emptyRow = list.querySelector('.empty');
  if (emptyRow) emptyRow.remove();

  const stick = nearBottom(list);
  const row = messageRow(msg);
  if (isOwn) row.classList.add('own');
  let placed = false;
  for (const sibling of list.children) {
    const siblingId = Number(sibling.dataset ? sibling.dataset.id : NaN);
    if (Number.isInteger(siblingId) && siblingId > msg.id) {
      list.insertBefore(row, sibling);
      placed = true;
      break;
    }
  }
  if (!placed) list.appendChild(row);

  seen.add(msg.id);
  if (stick) list.scrollTop = list.scrollHeight;
}

// mergeMessage renders msg unless its id is already on the page, inserting it
// in id order. Keying on the server id is what keeps stream delivery and a
// backfill response that was in flight at the same time from double-rendering
// the same message.
function mergeMessage(msg) {
  mergeInto(messagesList, seenIds, msg, false);
  if (msg && Number.isInteger(msg.id) && msg.id > lastSeenId) lastSeenId = msg.id;
}

// ---- seed from the server-rendered history ----

for (const row of messagesList.querySelectorAll('.msg')) {
  const id = Number(row.dataset.id);
  if (!Number.isInteger(id)) continue;
  seenIds.add(id);
  if (id > lastSeenId) lastSeenId = id;
  const time = row.querySelector('time[data-created-at]');
  if (time) time.textContent = formatTime(Number(time.dataset.createdAt));
}
messagesList.scrollTop = messagesList.scrollHeight;

// ---- stream ----

const events = new EventSource('/events?topic=room');

// parseEventData decodes one SSE event's JSON data, returning null when the
// payload is not valid JSON; every consumer treats null as "ignore".
function parseEventData(ev) {
  try {
    return JSON.parse(ev.data);
  } catch (err) {
    return null;
  }
}

events.addEventListener('connected', () => {
  connBanner.hidden = true;
  backfill();
  joinPresence();
});

events.addEventListener('message', (ev) => { mergeMessage(parseEventData(ev)); });
events.addEventListener('presence', (ev) => { renderOnline(parseEventData(ev)); });

events.onerror = () => {
  // EventSource retries on its own; the banner stays up until the next
  // connected event, which also backfills whatever was missed and re-joins
  // presence.
  connBanner.hidden = false;
};

// backfill fetches everything newer than the newest rendered message. It
// runs on the initial connection and after every reconnect, so messages
// published while the stream was down are merged in keyed on their server
// ids — including the sender's own messages, which render only here and on
// the stream, never optimistically.
async function backfill() {
  try {
    const res = await fetch('/api/messages?after=' + lastSeenId);
    if (!res.ok) return;
    const messages = await res.json();
    if (!Array.isArray(messages)) return;
    for (const msg of messages) mergeMessage(msg);
  } catch (err) {
    // Offline mid-reconnect; the next connected event retries.
  }
}

// ---- presence ----

// renderOnline rebuilds the online list from a presence snapshot: the join
// reply and every "presence" stream event both carry the full list, so a
// plain replace converges. Display names (already disambiguated server-side
// as name#xxxx for duplicate usernames) are user content: textContent only.
// Clicking another user opens (or creates) a DM conversation with them; your
// own entry is inert — no self-DMs.
let lastSnapshot = null;

function renderOnline(snapshot) {
  if (!snapshot || !Array.isArray(snapshot.online)) return;
  lastSnapshot = snapshot;
  const rows = [];
  for (const user of snapshot.online) {
    const self = Boolean(me && user.id === me.id);
    if (user.display || user.name) names.set(user.id, user.display || user.name);
    const row = document.createElement('li');
    row.dataset.id = String(user.id);
    row.textContent = (user.display || user.name || '') + (self ? ' (you)' : '');
    if (self) {
      row.className = 'self';
    } else {
      row.className = 'online-row';
      row.addEventListener('click', () => openConversation(user.id));
    }
    rows.push(row);
  }
  onlineList.replaceChildren(...rows);
  renderConversationList();
}

// joinPresence announces the caller and renders the list from the reply.
// Running it on every connected event (initial and each reconnect) makes
// presence self-heal after server restarts: a fresh stream always re-joins.
async function joinPresence() {
  try {
    const res = await fetch('/api/presence/join', { method: 'POST' });
    if (!res.ok) return;
    renderOnline(await res.json());
  } catch (err) {
    // Offline mid-reconnect; the next connected event retries the join.
  }
}

// sendHeartbeat refreshes the server-side lastSeen; nobody reads the reply.
function sendHeartbeat() {
  fetch('/api/presence/heartbeat', { method: 'POST' }).catch(() => {});
}

// One interval for the page's lifetime. 15s of cadence against the server's
// 45s TTL tolerates a couple of dropped beats; the visibilitychange nudge
// below covers background tabs, where browsers throttle timers.
setInterval(sendHeartbeat, 15000);

document.addEventListener('visibilitychange', () => {
  // Re-join rather than plain-heartbeat when the tab becomes visible: a join
  // also resurrects the entry if it expired while hidden, and repaints the
  // online list from the reply.
  if (document.visibilityState === 'visible') joinPresence();
});

// Leaving: navigator.sendBeacon issues a plain same-origin POST with the
// session cookie attached and no body to read, which /api/presence/leave
// accepts. Both unload events can fire for one navigation; the server treats
// a duplicate beacon as a no-op, so sending from both is safe.
function leavePresence() {
  if (navigator.sendBeacon) navigator.sendBeacon('/api/presence/leave');
}
window.addEventListener('beforeunload', leavePresence);
window.addEventListener('pagehide', leavePresence);

// ---- composer ----

// makeErrorFlasher returns a show(text) function bound to one error element
// and its own timer: the text shows now and hides again after four seconds,
// with a rapid second error restarting the clock.
function makeErrorFlasher(element) {
  let timer = 0;
  return (text) => {
    element.textContent = text;
    element.hidden = false;
    clearTimeout(timer);
    timer = setTimeout(() => { element.hidden = true; }, 4000);
  };
}

const showError = makeErrorFlasher(composerError);

composerForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const body = composerInput.value;
  try {
    const res = await fetch('/api/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ body: body }),
    });
    if (!res.ok) {
      let message = 'send failed (status ' + res.status + ')';
      try { message = (await res.json()).error || message; } catch (err) { /* non-JSON body */ }
      showError(message);
      return; // keep the drafted text; the server rejected it
    }
    composerInput.value = ''; // the message arrives via the stream
    composerInput.focus();
  } catch (err) {
    showError('network error: ' + err);
  }
});

composerInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    composerForm.requestSubmit();
  }
});

// ---- direct messages ----

// The caller's identity arrives from /api/me: it carries the one private
// piece of DM state the page needs — the dm topic to subscribe to. Until it
// resolves, incoming DM events have nowhere to route and are ignored (the
// open-conversation fetch and this stream re-deliver them).
let me = null;

// conversations maps peer user id -> {unread, lastId, routed}: one entry per
// correspondent row. routed dedupes stream delivery per conversation; the
// open pane keeps its own seen set so reopening a conversation re-renders
// its history.
const conversations = new Map();
const names = new Map(); // user id -> display name, from snapshots and lookups
let openPeer = null;
let dmPaneSeen = new Set();

async function loadMe() {
  try {
    const res = await fetch('/api/me');
    if (!res.ok) return false;
    const user = await res.json();
    if (!user || !user.id || !user.dm_topic) return false;
    me = user;
    openDMStream(me.dm_topic);
    if (lastSnapshot) renderOnline(lastSnapshot); // mark "(you)" retroactively
    return true;
  } catch (err) {
    // Session or network trouble; the retry loop below decides when to give up.
    return false;
  }
}

// loadMeWithRetries retries /api/me with capped backoff: one failed fetch at
// page load must not permanently disable receiving DMs — nothing else ever
// opens the dm stream. Success stops the loop; after the final attempt the
// DM pane shows a non-fatal notice (the room keeps working; a reload retries
// from scratch).
async function loadMeWithRetries() {
  const delays = [1000, 2000, 4000, 8000, 16000];
  for (let attempt = 0; attempt <= delays.length; attempt++) {
    if (await loadMe()) return;
    if (attempt < delays.length) {
      await new Promise((resolve) => setTimeout(resolve, delays[attempt]));
    }
  }
  showDMNotice('direct messages unavailable - reload to retry');
}

// showDMNotice swaps the DM pane's placeholder for a persistent notice row
// (textContent only). The pane cannot be opened without /api/me, so the row
// stays until reload.
function showDMNotice(text) {
  const row = document.createElement('li');
  row.className = 'empty';
  row.textContent = text;
  dmMessages.replaceChildren(row);
}
loadMeWithRetries();

// openDMStream subscribes to the caller's private topic. The dm stream is
// separate from the room stream, so a DM never renders in the room and a
// room message never touches a conversation.
function openDMStream(topic) {
  const stream = new EventSource('/events?topic=' + encodeURIComponent(topic));

  stream.addEventListener('connected', () => {
    // Heal whatever the missed deliveries were: refetch every known
    // conversation's history — the open one merges into the pane, the rest
    // fold into their conversation state with unread bumps. A first-ever DM
    // from a peer with no conversation yet cannot be discovered by these
    // refetches; it still needs the live event.
    for (const peer of conversations.keys()) loadConversation(peer);
  });

  stream.addEventListener('message', (ev) => { routeDM(parseEventData(ev)); });
}

// routeDM derives the correspondent from one payload shape shared by both
// topics: the peer is the author unless I am the author, in which case it is
// the recipient. Own copies and the other side's messages alike create their
// conversation row; unread bumps only for messages from someone else while
// that conversation is not open.
function routeDM(msg) {
  if (!me || !msg || !Number.isInteger(msg.id)) return;
  const peer = msg.author_id === me.id ? msg.to : msg.author_id;
  if (!peer) return;
  const conv = ensureConversation(peer);
  if (conv.routed.has(msg.id)) return;
  conv.routed.add(msg.id);
  conv.lastId = msg.id;
  if (peer === openPeer) {
    mergeDM(msg);
  } else if (msg.author_id !== me.id) {
    conv.unread += 1;
  }
  renderConversationList();
}

// ensureConversation creates the correspondent's row state on first sight
// and resolves their display name (snapshot first, public lookup second).
function ensureConversation(peer) {
  let conv = conversations.get(peer);
  if (!conv) {
    conv = { unread: 0, lastId: 0, routed: new Set() };
    conversations.set(peer, conv);
    lookupName(peer);
  }
  return conv;
}

// lookupName fills the names map for a user the snapshots have not shown —
// the sender of a DM that arrives after they already went offline.
async function lookupName(peer) {
  if (names.has(peer)) return;
  try {
    const res = await fetch('/api/users/' + encodeURIComponent(peer));
    if (!res.ok) return;
    const user = await res.json();
    const name = user && (user.display_name || user.username);
    if (!name) return;
    names.set(peer, name);
    if (peer === openPeer) dmTitle.textContent = name;
    renderConversationList();
  } catch (err) {
    // Offline; the next snapshot or open refetches the header.
  }
}

// renderConversationList rebuilds the sidebar rows: most recent conversation
// first, the open one highlighted, unread counts as badges.
function renderConversationList() {
  const peers = [...conversations.keys()];
  peers.sort((a, b) => (conversations.get(b).lastId || 0) - (conversations.get(a).lastId || 0));
  const rows = [];
  for (const peer of peers) {
    const conv = conversations.get(peer);
    const row = document.createElement('li');
    row.dataset.id = String(peer);
    row.className = peer === openPeer ? 'conv-row active' : 'conv-row';

    const name = document.createElement('span');
    name.className = 'conv-name';
    name.textContent = names.get(peer) || '…';
    row.append(name);

    if (conv.unread > 0) {
      const badge = document.createElement('span');
      badge.className = 'badge';
      badge.textContent = conv.unread > 99 ? '99+' : String(conv.unread);
      row.append(badge);
    }

    row.addEventListener('click', () => openConversation(peer));
    rows.push(row);
  }
  dmList.replaceChildren(...rows);
}

// mergeDM renders msg in the open conversation's pane unless its id is
// already there, inserting in id order — same rules as mergeMessage. Own
// messages align right.
function mergeDM(msg) {
  mergeInto(dmMessages, dmPaneSeen, msg, Boolean(me && msg && msg.author_id === me.id));
}

// openConversation switches the DM pane to one correspondent: clears unread,
// resets the pane's seen set, and loads the pair's history through the same
// merge path live events use. Clicking an online user and clicking a row
// both land here, so rows are created on demand.
async function openConversation(peer) {
  if (!me || peer === me.id) return; // no self-DMs
  openPeer = peer;
  const conv = ensureConversation(peer);
  conv.unread = 0;

  dmTitle.textContent = names.get(peer) || '…';
  lookupName(peer);
  dmInput.disabled = false;
  dmSend.disabled = false;

  dmPaneSeen = new Set();
  dmMessages.replaceChildren();
  const emptyRow = document.createElement('li');
  emptyRow.className = 'empty';
  emptyRow.textContent = 'No messages yet — say hello.';
  dmMessages.appendChild(emptyRow);
  dmMessages.scrollTop = 0;

  renderConversationList();
  await loadConversation(peer);
  dmInput.focus();
}

// loadConversation fetches the pair's history. For the open conversation it
// merges rows into the pane; for any other conversation it folds each
// message into that conversation's routed/lastId/unread state — the same
// bookkeeping routeDM does — without rendering, since the pane rebuilds from
// history when the conversation is opened. It runs on open and on every
// dm-stream (re)connect, so a stream that was down heals by refetch — the
// same backfill idea the room uses.
async function loadConversation(peer) {
  try {
    const res = await fetch('/api/dm?with=' + encodeURIComponent(peer));
    if (!res.ok) return;
    const messages = await res.json();
    if (!Array.isArray(messages)) return;
    if (peer === openPeer) {
      for (const msg of messages) mergeDM(msg);
      return;
    }
    const conv = conversations.get(peer);
    if (!conv) return;
    let changed = false;
    for (const msg of messages) {
      if (!msg || !Number.isInteger(msg.id) || conv.routed.has(msg.id)) continue;
      conv.routed.add(msg.id);
      conv.lastId = msg.id;
      if (me && msg.author_id !== me.id) conv.unread += 1;
      changed = true;
    }
    if (changed) renderConversationList();
  } catch (err) {
    // Offline mid-reconnect; the next connected event retries.
  }
}

const showDMError = makeErrorFlasher(dmError);

dmComposerForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (!me || !openPeer) return;
  const body = dmInput.value;
  try {
    const res = await fetch('/api/dm', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: openPeer, body: body }),
    });
    if (!res.ok) {
      let message = 'send failed (status ' + res.status + ')';
      if (res.status === 409) {
        message = 'user went offline — message not sent';
      } else if (res.status === 404) {
        message = 'user not found';
      } else {
        try { message = (await res.json()).error || message; } catch (err) { /* non-JSON body */ }
      }
      showDMError(message);
      return; // keep the drafted text; the server rejected it
    }
    dmInput.value = ''; // the own copy arrives via the dm stream
    dmInput.focus();
  } catch (err) {
    showDMError('network error: ' + err);
  }
});

dmInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    dmComposerForm.requestSubmit();
  }
});

// ---- logout ----

logoutButton.addEventListener('click', async () => {
  // Leave first, while the session cookie still validates the beacon: the
  // /api/logout call below clears it, and the pagehide beacon fired after
  // navigation would 401 and leave the user "online" until the server's TTL
  // reaps the entry.
  leavePresence();
  try {
    await fetch('/api/logout', { method: 'POST' });
  } catch (err) {
    // Redirect anyway; the cookie will simply expire unused.
  }
  window.location.href = '/login';
});
