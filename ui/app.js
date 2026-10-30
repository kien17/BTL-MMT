const state = {
  data: null,
  lastMessageKey: "",
  lastTransferKey: "",
};

const $ = (id) => document.getElementById(id);

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleTimeString();
}

function formatBytes(value) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(2)} MB`;
}

async function api(path, options = {}) {
  const response = await fetch(path, options);
  let payload = {};
  try {
    payload = await response.json();
  } catch {
    payload = {};
  }
  if (!response.ok) {
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

async function poll() {
  try {
    const data = await api("/api/state");
    state.data = data;
    render(data);
    $("global-error").textContent = data.error || "";
  } catch (error) {
    $("global-error").textContent = error.message;
  } finally {
    setTimeout(poll, 700);
  }
}

function render(data) {
  const online = Boolean(data.logged_in);
  $("login-panel").classList.toggle("hidden", online);
  $("app-panel").classList.toggle("hidden", !online);
  $("status-text").textContent = data.status || "disconnected";
  $("user-text").textContent = data.username ? `· ${data.username}` : "";
  $("status-dot").className = `dot ${data.status === "online" ? "online" : data.status === "connecting" ? "connecting" : ""}`;
  if (!online) return;

  renderPeers(data.peers || []);
  renderMessages(data.messages || []);
  renderTransfers(data.transfers || []);
}

function renderPeers(peers) {
  const list = $("peer-list");
  list.innerHTML = peers
    .map((peer) => `<li title="${escapeHtml(peer.username)}">${escapeHtml(peer.username)}</li>`)
    .join("");

  const select = $("send-to");
  const current = select.value;
  const options = ['<option value="*">Broadcast</option>'].concat(
    peers
      .filter((peer) => peer.username !== state.data.username)
      .map((peer) => `<option value="${escapeHtml(peer.username)}">${escapeHtml(peer.username)}</option>`)
  );
  select.innerHTML = options.join("");
  if ([...select.options].some((option) => option.value === current)) {
    select.value = current;
  }
}

function renderMessages(messages) {
  const key = messages
    .map((message) => `${message.msg_id}:${message.status || ""}:${message.offline ? 1 : 0}`)
    .join("|");
  if (key === state.lastMessageKey) return;
  state.lastMessageKey = key;

  const container = $("messages");
  const nearBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 40;
  container.innerHTML = messages
    .map((message) => {
      const mine = message.from === state.data.username;
      const system = message.kind === "system";
      const classes = ["message", mine ? "mine" : "", system ? "system" : ""].join(" ");
      const meta = system
        ? ""
        : `<div class="meta"><span>${escapeHtml(message.from)} → ${escapeHtml(message.to)}</span><span>${formatTime(message.ts)}</span>${message.offline ? '<span class="badge">offline</span>' : ""}${message.status ? `<span>${escapeHtml(message.status)}</span>` : ""}</div>`;
      return `<div class="${classes}">${escapeHtml(message.text)}${meta}</div>`;
    })
    .join("");
  if (nearBottom) {
    container.scrollTop = container.scrollHeight;
  }
}

function renderTransfers(transfers) {
  const key = JSON.stringify(transfers);
  if (key === state.lastTransferKey) return;
  state.lastTransferKey = key;

  const container = $("transfer-list");
  container.innerHTML = transfers
    .map((transfer) => {
      const percent = transfer.size > 0 ? Math.min(100, Math.round((transfer.progress / transfer.size) * 100)) : 0;
      const direction = transfer.direction === "send" ? "↑" : "↓";
      const download = transfer.direction === "recv" && transfer.status === "completed"
        ? `<a href="/api/download/${transfer.transfer_id}">download</a>`
        : "";
      return `
        <div class="transfer">
          <div class="title"><span>${direction} ${escapeHtml(transfer.filename)}</span><span>${percent}%</span></div>
          <div class="bar"><span style="width:${percent}%"></span></div>
          <div class="detail">${escapeHtml(transfer.peer)} · ${formatBytes(transfer.progress)} / ${formatBytes(transfer.size)} · ${escapeHtml(transfer.status)} ${download}</div>
          ${transfer.error ? `<div class="detail error">${escapeHtml(transfer.error)}</div>` : ""}
        </div>`;
    })
    .join("");
}

$("login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("login-error").textContent = "";
  try {
    await api("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: $("login-username").value,
        password: $("login-password").value,
      }),
    });
    state.lastMessageKey = "";
    state.lastTransferKey = "";
  } catch (error) {
    $("login-error").textContent = error.message;
  }
});

$("send-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const to = $("send-to").value;
  const text = $("send-text").value.trim();
  if (!to || !text) return;
  try {
    await api("/api/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to, text }),
    });
    $("send-text").value = "";
    state.lastMessageKey = "";
  } catch (error) {
    $("global-error").textContent = error.message;
  }
});

$("file-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const file = $("file-input").files[0];
  const to = $("send-to").value;
  if (!file) return;
  if (to === "*") {
    $("global-error").textContent = "Chọn một người nhận cụ thể để gửi file.";
    return;
  }
  try {
    await api("/api/upload", {
      method: "POST",
      headers: {
        "X-Recipient": to,
        "X-Filename": encodeURIComponent(file.name),
      },
      body: file,
    });
    $("file-input").value = "";
    state.lastTransferKey = "";
  } catch (error) {
    $("global-error").textContent = error.message;
  }
});

$("reconnect-btn").addEventListener("click", async () => {
  try {
    await api("/api/reconnect", { method: "POST" });
    state.lastMessageKey = "";
    state.lastTransferKey = "";
  } catch (error) {
    $("global-error").textContent = error.message;
  }
});

$("logout-btn").addEventListener("click", async () => {
  try {
    await api("/api/logout", { method: "POST" });
  } catch (error) {
    $("global-error").textContent = error.message;
  }
});

poll();
