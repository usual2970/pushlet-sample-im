'use strict';

// sample-im room client. Subscribes to the fixed "room" SSE topic, merges
// stream messages with the server-rendered history keyed on server message
// ids, and posts new messages to /api/messages. Security rule (KTD12): every
// piece of dynamic text is set through textContent — user content never
// touches innerHTML.

const messagesList = document.getElementById('messages');
const composerForm = document.getElementById('composer');
const composerInput = document.getElementById('composer-input');
const composerError = document.getElementById('composer-error');
const connBanner = document.getElementById('conn-banner');
const logoutButton = document.getElementById('logout');

let lastSeenId = 0;         // highest server message id rendered
const seenIds = new Set();  // every server message id rendered
let errorTimer = 0;

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

function nearBottom() {
  return messagesList.scrollHeight - messagesList.scrollTop - messagesList.clientHeight < 120;
}

// mergeMessage renders msg unless its id is already on the page, inserting it
// in id order. Keying on the server id is what keeps stream delivery and a
// backfill response that was in flight at the same time from double-rendering
// the same message.
function mergeMessage(msg) {
  if (!msg || !Number.isInteger(msg.id) || seenIds.has(msg.id)) return;

  const emptyRow = messagesList.querySelector('.empty');
  if (emptyRow) emptyRow.remove();

  const stick = nearBottom();
  const row = messageRow(msg);
  let placed = false;
  for (const sibling of messagesList.children) {
    const siblingId = Number(sibling.dataset ? sibling.dataset.id : NaN);
    if (Number.isInteger(siblingId) && siblingId > msg.id) {
      messagesList.insertBefore(row, sibling);
      placed = true;
      break;
    }
  }
  if (!placed) messagesList.appendChild(row);

  seenIds.add(msg.id);
  if (msg.id > lastSeenId) lastSeenId = msg.id;
  if (stick) messagesList.scrollTop = messagesList.scrollHeight;
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

events.addEventListener('connected', () => {
  connBanner.hidden = true;
  backfill();
});

events.addEventListener('message', (ev) => {
  let msg;
  try {
    msg = JSON.parse(ev.data);
  } catch (err) {
    return;
  }
  mergeMessage(msg);
});

events.onerror = () => {
  // EventSource retries on its own; the banner stays up until the next
  // connected event, which also backfills whatever was missed.
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

// ---- composer ----

function showError(text) {
  composerError.textContent = text;
  composerError.hidden = false;
  clearTimeout(errorTimer);
  errorTimer = setTimeout(() => { composerError.hidden = true; }, 4000);
}

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

// ---- logout ----

logoutButton.addEventListener('click', async () => {
  try {
    await fetch('/api/logout', { method: 'POST' });
  } catch (err) {
    // Redirect anyway; the cookie will simply expire unused.
  }
  window.location.href = '/login';
});
