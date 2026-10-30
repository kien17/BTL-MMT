# LAN Collaboration — Distributed Streaming Client-Server

Ứng dụng cộng tác self-hosted cho mạng LAN/VPN: đăng nhập, presence, chat broadcast/unicast, gửi file, offline message, mã hoá payload.

- **TCP control plane**: handshake, ECDH, auth, presence, heartbeat.
- **UDP data plane**: chat/file chunk + reliability tự viết (sliding window, SACK, RTO, retransmit, congestion control).
- **UI**: HTTP server tối giản + JavaScript thuần, polling (không WebSocket).
- **Storage**: SQLite cho users và offline messages.
- **Security**: X25519 + HKDF-SHA256 + AES-256-GCM; password scrypt.

Tài liệu kiến trúc chi tiết: [`docs/architecture.md`](docs/architecture.md).

---

## 1. Yêu cầu môi trường

- Python **3.11+** (đã test với 3.14.8).
- Linux/macOS/Windows.
- Không cần database server, không cần internet khi chạy demo.

---

## 2. Cài đặt

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

Nếu chỉ chạy ứng dụng (không test):

```bash
python -m pip install -r requirements.txt
```

---

## 3. Chạy server

```bash
source .venv/bin/activate
python scripts/run_server.py
```

Mặc định:

- TCP: `0.0.0.0:9000`
- UDP: `0.0.0.0:9001`
- SQLite: `server/data/app.db`

Server tự seed user demo:

| Username | Password |
|---|---|
| `alice` | `password123` |
| `bob` | `password123` |
| `charlie` | `password123` |

Có thể đổi cấu hình trong [`config.toml`](config.toml) hoặc bằng biến môi trường (`DB_PATH`, `SERVER_BIND_HOST`, `SERVER_TCP_PORT`, `SERVER_UDP_PORT`).

---

## 4. Chạy client + UI

Mỗi client chạy một tiến trình riêng, UI ở cổng khác nhau.

```bash
# Terminal 2
source .venv/bin/activate
python scripts/run_client.py --username alice --password password123 --ui-port 8080

# Terminal 3
source .venv/bin/activate
python scripts/run_client.py --username bob --password password123 --ui-port 8081
```

Mở trình duyệt:

- Alice: <http://127.0.0.1:8080>
- Bob: <http://127.0.0.1:8081>

Nếu không truyền `--username/--password`, UI sẽ hiện form đăng nhập.

### Tham số client

```bash
python scripts/run_client.py \
  --server-host 127.0.0.1 \
  --server-tcp-port 9000 \
  --server-udp-port 9001 \
  --ui-port 8080 \
  --download-dir downloads \
  --username alice \
  --password password123
```

File nhận được lưu trong `downloads/`; có thể tải lại từ UI.

---

## 5. Tính năng demo

1. Đăng nhập 2–3 client → thấy peer list cập nhật realtime (polling).
2. Chat broadcast (`Broadcast`) hoặc chọn một peer để chat riêng.
3. Gửi file: chọn recipient cụ thể, chọn file, bấm **Send file**; theo dõi progress bar.
4. Offline message:
   - Alice gửi tin nhắn cho Bob khi Bob chưa online.
   - Bob login → nhận tin nhắn cũ (đánh dấu `offline`).
5. Đóng client Bob → Alice thấy Bob offline; gửi tiếp sẽ vào queue.
6. Reconnect: bấm **Reconnect** trên UI.
7. Encryption: payload TCP/UDP được mã hoá bằng AES-256-GCM; có thể kiểm tra bằng log/packet capture (nội dung không đọc được).

---

## 6. Test

```bash
source .venv/bin/activate
pytest -q
ruff check .
```

Test hiện có:

- `tests/test_protocol.py`: header, CRC, TCP frame, app reassembler, AES-GCM.
- `tests/test_crypto.py`: X25519, scrypt, SHA-256.
- `tests/test_reliability.py`: lossless, mất gói + retransmit, không HoL giữa stream, file stream.

### Mô phỏng mất gói

Chạy proxy UDP mất gói 20% (single client) giữa client và server:

```bash
# Terminal A: server
python scripts/run_server.py

# Terminal B: proxy
python scripts/lossy_proxy.py --listen-port 9101 --server-port 9001 --drop 0.2

# Terminal C: client dùng proxy làm UDP port
python scripts/run_client.py --username alice --password password123 \
  --server-udp-port 9101 --ui-port 8080
```

Lưu ý: proxy hiện hỗ trợ một client tại một thời điểm. Với nhiều client, dùng mạng thật hoặc `tc netem` trên Linux.

---

## 7. Cấu trúc chính

```text
common/     protocol, crypto, reliability, congestion, app framing, config
server/     TCP/UDP listeners, session, auth, storage, router, relay
client/     control, data, file transfer, core, HTTP UI server
ui/         index.html, app.js, style.css
scripts/    run_server.py, run_client.py, lossy_proxy.py
tests/      unit tests
docs/       architecture.md
```

---

## 8. Ghi chú thiết kế

- Đề yêu cầu UDP streaming nhưng rubric Step 2 yêu cầu TCP handshake → dùng kiến trúc hybrid: TCP control + UDP data.
- Reliability chạy **hop-by-hop** (client↔server, server↔client). Server relay nhận đúng thứ tự rồi forward.
- Mỗi message/file dùng `stream_id` riêng → tránh Head-of-Line blocking.
- File gửi cho user offline bị từ chối (`recipient_offline`); chỉ text unicast được queue.
- Server relay thấy plaintext sau khi decrypt (mô hình client-server). Không có PKI nên chống MITM hạn chế; có thể pin server key nếu cần.

---

## 9. Troubleshooting

| Lỗi | Cách xử lý |
|---|---|
| `Address already in use` | Đổi port trong `config.toml` hoặc dùng `--ui-port` khác |
| Client không lên `online` | Kiểm tra server đang chạy, TCP/UDP port đúng, firewall |
| UI không cập nhật | Kiểm tra terminal client có lỗi; thử bấm Reconnect |
| File không nhận được | Kiểm tra recipient online, dung lượng < 100MB, thư mục `downloads/` ghi được |
| `cryptography` cài lỗi | Dùng Python 3.11–3.13 hoặc cài wheel phù hợp |

---

## 10. Docker (optional)

Chưa bắt buộc cho demo. Có thể thêm sau:

```bash
docker compose up --build
```

Nếu dùng Docker, nên chạy server và client trong cùng compose network; client ngoài Docker cần cấu hình UDP port mapping cẩn thận.
