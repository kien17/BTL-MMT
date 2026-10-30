const state = {
  data: null,
  selectedTarget: "*",
  filterByTarget: true,
  searchQuery: "",
  seenMsgIds: new Set(),
  unreadByTarget: {},
  initializedSeen: false,
  lastRenderSignature: "",
  lastTransferKey: "",
};

const $ = (id) => document.getElementById(id);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatBytes(value) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(2)} MB`;
}

function avatarColorClass(username) {
  const name = String(username || "").toLowerCase();
  if (name === "alice") return "avatar-alice";
  if (name === "bob") return "avatar-bob";
  if (name === "charlie") return "avatar-charlie";
  return "";
}

function avatarInitial(username) {
  if (!username || username === "*") return "📢";
  return String(username).charAt(0).toUpperCase();
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
    setTimeout(poll, 650);
  }
}

function conversationKeyForMessage(message, myUsername) {
  if (message.kind === "system") return "*";
  if (message.to === "*") return "*";
  return message.from === myUsername ? message.to : message.from;
}

function updateUnreadCounts(messages, myUsername) {
  if (!state.initializedSeen) {
    for (const msg of messages) {
      if (msg.msg_id) state.seenMsgIds.add(msg.msg_id);
    }
    state.initializedSeen = true;
    return;
  }
  for (const msg of messages) {
    if (!msg.msg_id || state.seenMsgIds.has(msg.msg_id)) continue;
    state.seenMsgIds.add(msg.msg_id);
    if (msg.kind === "system" || msg.from === myUsername) continue;
    const convKey = conversationKeyForMessage(msg, myUsername);
    if (!state.filterByTarget || state.selectedTarget === convKey) continue;
    state.unreadByTarget[convKey] = (state.unreadByTarget[convKey] || 0) + 1;
  }
}

function selectTarget(target) {
  state.selectedTarget = target || "*";
  state.unreadByTarget[state.selectedTarget] = 0;
  const select = $("send-to");
  if (select && [...select.options].some((opt) => opt.value === state.selectedTarget)) {
    select.value = state.selectedTarget;
  }
  state.lastRenderSignature = "";
  if (state.data) {
    render(state.data);
  }
}

function formatStatusLabel(status) {
  if (status === "online") return "Trực tuyến (Online)";
  if (status === "offline") return "Chế độ Offline (Queue bật)";
  if (status === "connecting") return "Đang kết nối...";
  return "Mất kết nối (Disconnected)";
}

function render(data) {
  const loggedIn = Boolean(data.logged_in);
  const isOnline = Boolean(data.online ?? data.status === "online");
  $("login-panel").classList.toggle("hidden", loggedIn);
  $("app-panel").classList.toggle("hidden", !loggedIn);

  $("status-text").textContent = formatStatusLabel(data.status || "disconnected");
  $("user-text").textContent = data.username ? `· ${data.username}` : "";
  $("status-dot").className = `dot ${
    isOnline ? "online" : data.status === "connecting" ? "connecting" : "offline"
  }`;

  $("offline-btn").classList.toggle("hidden", !loggedIn || !isOnline);
  $("reconnect-btn").classList.toggle("hidden", !loggedIn || isOnline);
  $("offline-banner").classList.toggle("hidden", !loggedIn || isOnline);

  if (!loggedIn) {
    state.initializedSeen = false;
    state.seenMsgIds.clear();
    state.unreadByTarget = {};
    return;
  }

  const messages = data.messages || [];
  const peers = data.peers || [];
  updateUnreadCounts(messages, data.username);

  renderPeersAndConversations(peers, messages, data.username);
  renderChatHeader(peers, data.username);
  renderMessages(messages, peers, data);
  renderQueuePanel(messages, peers, data);
  renderTransfers(data.transfers || []);
}

function getLastMessageForTarget(messages, myUsername, target) {
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.kind === "system") continue;
    if (target === "*" && msg.to === "*") return msg;
    if (
      target !== "*" &&
      msg.to !== "*" &&
      ((msg.from === myUsername && msg.to === target) ||
        (msg.from === target && msg.to === myUsername))
    ) {
      return msg;
    }
  }
  return null;
}

function renderPeersAndConversations(peers, messages, myUsername) {
  const otherPeers = peers.filter((peer) => peer.username !== myUsername);
  const onlineCount = otherPeers.filter((peer) => peer.online !== false).length;
  $("online-counter").textContent = `${onlineCount}/${otherPeers.length} online`;

  // Populate recipient <select> with Broadcast + ALL peers (both Online and Offline!)
  const select = $("send-to");
  const options = ['<option value="*">📢 Broadcast (Tất cả)</option>'].concat(
    otherPeers.map((peer) => {
      const isOnline = peer.online !== false;
      const statusSuffix = isOnline ? "🟢 Online" : "🌙 Offline (Vào Queue)";
      return `<option value="${escapeHtml(peer.username)}">${escapeHtml(peer.username)} — ${statusSuffix}</option>`;
    })
  );
  const optionsHtml = options.join("");
  if (select.innerHTML !== optionsHtml) {
    select.innerHTML = optionsHtml;
  }
  if ([...select.options].some((option) => option.value === state.selectedTarget)) {
    select.value = state.selectedTarget;
  } else {
    state.selectedTarget = "*";
    select.value = "*";
  }

  const query = state.searchQuery.trim().toLowerCase();
  const list = $("peer-list");

  const itemsHtml = [];

  // 1. Broadcast Channel item
  if (!query || "broadcast kênh chung tất cả".includes(query)) {
    const lastBroadcast = getLastMessageForTarget(messages, myUsername, "*");
    const preview = lastBroadcast
      ? `${lastBroadcast.from === myUsername ? "Bạn" : lastBroadcast.from}: ${lastBroadcast.text}`
      : "Trò chuyện chung toàn mạng";
    const unread = state.unreadByTarget["*"] || 0;
    itemsHtml.push(`
      <li class="conv-section-label">Kênh chung</li>
      <li class="conv-item ${state.selectedTarget === "*" ? "active" : ""}" data-target="*">
        <div class="avatar broadcast">📢</div>
        <div class="conv-body">
          <div class="conv-top">
            <span class="conv-name">Kênh Chung (Broadcast)</span>
            <span class="conv-state-tag online">All</span>
          </div>
          <div class="conv-bottom">
            <span class="conv-preview">${escapeHtml(preview)}</span>
            <div class="conv-badges">
              ${unread > 0 ? `<span class="unread-badge">${unread}</span>` : ""}
            </div>
          </div>
        </div>
      </li>
    `);
  }

  // 2. Direct Message Peers (Online + Offline)
  const filteredPeers = otherPeers.filter((peer) =>
    !query ? true : peer.username.toLowerCase().includes(query)
  );

  itemsHtml.push(`<li class="conv-section-label">Tin nhắn trực tiếp (${filteredPeers.length})</li>`);

  for (const peer of filteredPeers) {
    const uname = peer.username;
    const isOnline = peer.online !== false;
    const lastMsg = getLastMessageForTarget(messages, myUsername, uname);
    let preview = isOnline
      ? "Đang trực tuyến · Nhấn để nhắn tin"
      : "Đang Offline · Nhắn tin sẽ lưu vào Queue";
    if (lastMsg) {
      const prefix = lastMsg.from === myUsername ? "Bạn: " : "";
      preview = `${prefix}${lastMsg.text}`;
    }
    const unread = state.unreadByTarget[uname] || 0;
    const localQueuedForPeer = messages.filter(
      (m) =>
        m.from === myUsername &&
        m.to === uname &&
        (m.status === "queued" || m.status === "queued_local")
    ).length;
    const queueCount = Math.max(peer.queued_count || 0, localQueuedForPeer);

    itemsHtml.push(`
      <li class="conv-item ${state.selectedTarget === uname ? "active" : ""}" data-target="${escapeHtml(uname)}">
        <div class="avatar ${avatarColorClass(uname)}">
          ${escapeHtml(avatarInitial(uname))}
          <span class="avatar-status ${isOnline ? "online" : "offline"}"></span>
        </div>
        <div class="conv-body">
          <div class="conv-top">
            <span class="conv-name">${escapeHtml(uname)}</span>
            <span class="conv-state-tag ${isOnline ? "online" : "offline"}">
              ${isOnline ? "🟢 Online" : "🌙 Offline"}
            </span>
          </div>
          <div class="conv-bottom">
            <span class="conv-preview">${escapeHtml(preview)}</span>
            <div class="conv-badges">
              ${queueCount > 0 ? `<span class="queue-mini-badge" title="Tin nhắn đang chờ trong Offline Queue">⏳ ${queueCount}</span>` : ""}
              ${unread > 0 ? `<span class="unread-badge">${unread}</span>` : ""}
            </div>
          </div>
        </div>
      </li>
    `);
  }

  list.innerHTML = itemsHtml.join("");
  list.querySelectorAll(".conv-item").forEach((item) => {
    item.addEventListener("click", () => {
      selectTarget(item.getAttribute("data-target") || "*");
    });
  });
}

function renderChatHeader(peers, myUsername) {
  const target = state.selectedTarget;
  const avatarEl = $("active-avatar");
  const titleEl = $("active-chat-title");
  const badgeEl = $("active-chat-badge");
  const subEl = $("active-chat-subtitle");
  const filterBtn = $("filter-mode-btn");

  filterBtn.textContent = state.filterByTarget
    ? "🔍 Đang lọc: Hội thoại này"
    : "🌐 Đang xem: Tất cả tin nhắn";

  if (target === "*") {
    avatarEl.className = "avatar broadcast";
    avatarEl.textContent = "📢";
    titleEl.textContent = "Kênh Chung (Broadcast)";
    badgeEl.className = "target-badge online";
    badgeEl.textContent = "Broadcast";
    subEl.textContent = "Tin nhắn gửi tới tất cả người dùng đang trực tuyến trên mạng LAN";
    return;
  }

  const peer = peers.find((p) => p.username === target);
  const isOnline = peer ? peer.online !== false : false;
  avatarEl.className = `avatar ${avatarColorClass(target)}`;
  avatarEl.textContent = avatarInitial(target);
  titleEl.textContent = `Trò chuyện với ${target}`;

  if (isOnline) {
    badgeEl.className = "target-badge online";
    badgeEl.textContent = "🟢 Đang Online";
    subEl.textContent = "Kết nối UDP trực tiếp · Tin nhắn sẽ được chuyển ngay lập tức";
  } else {
    badgeEl.className = "target-badge offline";
    badgeEl.textContent = "🌙 Đang Offline · Queue tự động";
    subEl.textContent =
      "Người nhận đang ngoại tuyến — bạn vẫn nhắn tin bình thường, Server sẽ lưu vào Offline Queue và tự gửi khi họ Online!";
  }
}

function renderStatusBadge(message, mine) {
  const badges = [];
  if (message.offline) {
    badges.push(
      '<span class="badge offline-received" title="Tin nhắn được gửi lúc bạn Offline và chuyển từ hàng đợi khi bạn kết nối lại">📥 Nhận từ Offline Queue</span>'
    );
  }
  if (mine && message.status) {
    const st = String(message.status);
    if (st === "queued") {
      badges.push(
        '<span class="badge status-queued" title="Người nhận đang Offline — tin nhắn đã được lưu an toàn vào hàng đợi trên Server">🕒 Đã lưu Offline Queue (Chờ người nhận Online)</span>'
      );
    } else if (st === "queued_local") {
      badges.push(
        '<span class="badge status-outbox" title="Bạn đang Offline — tin nhắn nằm trong Outbox và sẽ tự động gửi ngay khi kết nối lại">⏳ Hàng đợi Outbox (Chờ kết nối mạng)</span>'
      );
    } else if (st === "delivered") {
      badges.push('<span class="badge status-delivered">✓✓ Đã nhận (Delivered)</span>');
    } else if (st.startsWith("broadcast:")) {
      const count = st.split(":")[1] || "0";
      badges.push(`<span class="badge status-delivered">📡 Đã phát (${escapeHtml(count)} peers)</span>`);
    } else if (st === "sending") {
      badges.push('<span class="badge status-sending">📤 Đang gửi UDP...</span>');
    } else {
      badges.push(`<span class="badge status-error">⚠️ ${escapeHtml(st)}</span>`);
    }
  }
  return badges.join("");
}

function renderMessages(messages, peers, data) {
  const myUsername = data.username;
  const target = state.selectedTarget;

  const filtered = messages.filter((msg) => {
    if (!state.filterByTarget) return true;
    if (msg.kind === "system") return true;
    if (target === "*") return msg.to === "*";
    return (
      msg.to !== "*" &&
      ((msg.from === myUsername && msg.to === target) ||
        (msg.from === target && msg.to === myUsername))
    );
  });

  const signature = [
    target,
    state.filterByTarget ? "1" : "0",
    ...filtered.map((m) => `${m.msg_id}:${m.status || ""}:${m.offline ? 1 : 0}`),
  ].join("|");

  if (signature === state.lastRenderSignature) return;
  state.lastRenderSignature = signature;

  const container = $("messages");
  const nearBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 60;

  if (filtered.length === 0) {
    const peerObj = peers.find((p) => p.username === target);
    const peerOffline = target !== "*" && (!peerObj || peerObj.online === false);
    container.innerHTML = `
      <div class="empty-chat">
        <div class="empty-chat-icon">${target === "*" ? "📢" : peerOffline ? "🌙" : "💬"}</div>
        <strong>Chưa có tin nhắn nào trong cuộc trò chuyện với ${escapeHtml(target === "*" ? "Kênh Broadcast" : target)}</strong>
        <p>${
          peerOffline
            ? `Hiện tại <b>${escapeHtml(target)}</b> đang Offline. Bạn cứ gửi tin nhắn bên dưới — hệ thống sẽ xếp vào <b>Offline Queue</b> và chuyển ngay khi ${escapeHtml(target)} đăng nhập!`
            : "Hãy gửi tin nhắn đầu tiên ở khung nhập bên dưới."
        }</p>
      </div>
    `;
    return;
  }

  container.innerHTML = filtered
    .map((message) => {
      if (message.kind === "system") {
        return `<div class="message system">${escapeHtml(message.text)} · ${formatTime(message.ts)}</div>`;
      }
      const mine = message.from === myUsername;
      const classes = [
        "message",
        mine ? "mine" : "",
        message.offline ? "offline-msg" : "",
      ]
        .filter(Boolean)
        .join(" ");

      const routeLabel =
        message.to === "*"
          ? `${escapeHtml(message.from)} → 📢 Broadcast`
          : `${escapeHtml(message.from)} → ${escapeHtml(message.to)}`;

      const badgesHtml = renderStatusBadge(message, mine);

      return `
        <div class="${classes}">
          <div class="msg-header">
            <span class="msg-route">${routeLabel}</span>
            <span class="msg-time">${formatTime(message.ts)}</span>
          </div>
          <div class="msg-text">${escapeHtml(message.text)}</div>
          ${badgesHtml ? `<div class="meta">${badgesHtml}</div>` : ""}
        </div>
      `;
    })
    .join("");

  if (nearBottom) {
    container.scrollTop = container.scrollHeight;
  }
}

function renderQueuePanel(messages, peers, data) {
  const myUsername = data.username;
  const outboxMessages = messages.filter(
    (m) => m.from === myUsername && m.status === "queued_local"
  );
  const serverQueuedMessages = messages.filter(
    (m) => m.from === myUsername && m.status === "queued"
  );
  const receivedOfflineMessages = messages.filter(
    (m) => m.from !== myUsername && Boolean(m.offline)
  );

  // Also include server-reported queued_count for peers
  const peerServerQueueSum = peers
    .filter((p) => p.username !== myUsername)
    .reduce((acc, p) => acc + (p.queued_count || 0), 0);
  const effectiveServerQueue = Math.max(serverQueuedMessages.length, peerServerQueueSum);

  $("stat-outbox").textContent = String(outboxMessages.length);
  $("stat-server-queue").textContent = String(effectiveServerQueue);
  $("stat-received-offline").textContent = String(receivedOfflineMessages.length);
  $("queue-total-badge").textContent = String(
    outboxMessages.length + effectiveServerQueue + receivedOfflineMessages.length
  );

  const cards = [];

  for (const msg of outboxMessages.slice().reverse()) {
    cards.push(`
      <div class="queue-card outbox">
        <div class="queue-card-top">
          <span>⏳ Outbox → ${escapeHtml(msg.to)}</span>
          <span>${formatTime(msg.ts)}</span>
        </div>
        <div class="queue-card-text">${escapeHtml(msg.text)}</div>
        <div class="queue-card-meta">
          <span>Đang chờ máy bạn Online</span>
          <span>Local Queue</span>
        </div>
      </div>
    `);
  }

  for (const msg of serverQueuedMessages.slice().reverse()) {
    cards.push(`
      <div class="queue-card">
        <div class="queue-card-top">
          <span>🕒 Chờ ${escapeHtml(msg.to)} online</span>
          <span>${formatTime(msg.ts)}</span>
        </div>
        <div class="queue-card-text">${escapeHtml(msg.text)}</div>
        <div class="queue-card-meta">
          <span>Đã lưu SQLite trên Server</span>
          <span>Server Queue</span>
        </div>
      </div>
    `);
  }

  for (const msg of receivedOfflineMessages.slice(-8).reverse()) {
    cards.push(`
      <div class="queue-card received">
        <div class="queue-card-top">
          <span>📥 Từ ${escapeHtml(msg.from)}</span>
          <span>${formatTime(msg.ts)}</span>
        </div>
        <div class="queue-card-text">${escapeHtml(msg.text)}</div>
        <div class="queue-card-meta">
          <span>Đã nhận khi Online lại</span>
          <span>Delivered</span>
        </div>
      </div>
    `);
  }

  const queueList = $("queue-list");
  if (cards.length === 0) {
    queueList.innerHTML = `
      <div class="empty-mini">
        Hàng đợi đang trống.<br>
        Hãy thử nhắn tin cho một người đang <b>Offline</b> hoặc bấm <b>🌙 Chuyển Offline</b> để kiểm thử Queue!
      </div>
    `;
  } else {
    queueList.innerHTML = cards.join("");
  }
}

function renderTransfers(transfers) {
  const key = JSON.stringify(transfers);
  if (key === state.lastTransferKey) return;
  state.lastTransferKey = key;

  const container = $("transfer-list");
  if (!transfers.length) {
    container.innerHTML = `<div class="empty-mini">Chưa có phiên truyền tệp nào.</div>`;
    return;
  }

  container.innerHTML = transfers
    .map((transfer) => {
      const percent =
        transfer.size > 0
          ? Math.min(100, Math.round((transfer.progress / transfer.size) * 100))
          : 0;
      const direction = transfer.direction === "send" ? "↑ Gửi" : "↓ Nhận";
      const download =
        transfer.direction === "recv" && transfer.status === "completed"
          ? `<a href="/api/download/${transfer.transfer_id}">⬇ Tải xuống</a>`
          : "";
      return `
        <div class="transfer">
          <div class="title">
            <span>${direction}: ${escapeHtml(transfer.filename)}</span>
            <span>${percent}%</span>
          </div>
          <div class="bar"><span style="width:${percent}%"></span></div>
          <div class="detail">
            <span>${escapeHtml(transfer.peer)} · ${formatBytes(transfer.progress)} / ${formatBytes(transfer.size)} · ${escapeHtml(transfer.status)}</span>
            ${download}
          </div>
          ${transfer.error ? `<div class="detail error">${escapeHtml(transfer.error)}</div>` : ""}
        </div>`;
    })
    .join("");
}

// Quick demo user selector on Login screen
document.querySelectorAll(".user-chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    document.querySelectorAll(".user-chip").forEach((c) => c.classList.remove("active"));
    chip.classList.add("active");
    const username = chip.getAttribute("data-user") || "alice";
    $("login-username").value = username;
    $("login-password").value = "password123";
  });
});

$("peer-search").addEventListener("input", (event) => {
  state.searchQuery = event.target.value || "";
  if (state.data) {
    renderPeersAndConversations(
      state.data.peers || [],
      state.data.messages || [],
      state.data.username
    );
  }
});

$("send-to").addEventListener("change", (event) => {
  selectTarget(event.target.value || "*");
});

$("filter-mode-btn").addEventListener("click", () => {
  state.filterByTarget = !state.filterByTarget;
  state.lastRenderSignature = "";
  if (state.data) {
    render(state.data);
  }
});

$("file-input").addEventListener("change", (event) => {
  const file = event.target.files[0];
  $("file-selected-name").textContent = file
    ? `${file.name} (${formatBytes(file.size)})`
    : "Chưa chọn tệp (tối đa 100MB, kiểm tra SHA-256)";
});

$("login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("login-error").textContent = "";
  try {
    await api("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: $("login-username").value.trim(),
        password: $("login-password").value,
      }),
    });
    state.lastRenderSignature = "";
    state.lastTransferKey = "";
  } catch (error) {
    $("login-error").textContent = error.message;
  }
});

$("send-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const to = $("send-to").value || state.selectedTarget || "*";
  const text = $("send-text").value.trim();
  if (!to || !text) return;
  try {
    await api("/api/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to, text }),
    });
    $("send-text").value = "";
    state.lastRenderSignature = "";
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
    $("global-error").textContent = "Vui lòng chọn một người nhận cụ thể (không phải Broadcast) để gửi file.";
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
    $("file-selected-name").textContent = "Chưa chọn tệp (tối đa 100MB, kiểm tra SHA-256)";
    state.lastTransferKey = "";
  } catch (error) {
    $("global-error").textContent = error.message;
  }
});

$("offline-btn").addEventListener("click", async () => {
  try {
    await api("/api/disconnect", { method: "POST" });
    state.lastRenderSignature = "";
  } catch (error) {
    $("global-error").textContent = error.message;
  }
});

async function handleReconnect() {
  try {
    await api("/api/reconnect", { method: "POST" });
    state.lastRenderSignature = "";
    state.lastTransferKey = "";
  } catch (error) {
    $("global-error").textContent = error.message;
  }
}

$("reconnect-btn").addEventListener("click", handleReconnect);
$("banner-reconnect-btn").addEventListener("click", handleReconnect);

$("logout-btn").addEventListener("click", async () => {
  try {
    await api("/api/logout", { method: "POST" });
    state.lastRenderSignature = "";
    state.lastTransferKey = "";
  } catch (error) {
    $("global-error").textContent = error.message;
  }
});

poll();
