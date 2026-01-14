import os
import json
import time
import uuid
import hashlib
import sqlite3
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify, abort, render_template_string

# -----------------------------
# Config (Railway env vars)
# -----------------------------
IMOU_DATACENTER = os.getenv("IMOU_DATACENTER", "").strip()   # e.g. "eu", "us", "cn" (depends on your Imou app)
IMOU_APP_ID = os.getenv("IMOU_APP_ID", "").strip()
IMOU_APP_SECRET = os.getenv("IMOU_APP_SECRET", "").strip()

ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()  # protect /admin/* endpoints
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")  # e.g. https://your-service.up.railway.app

IMOU_CALLBACK_FLAGS = os.getenv("IMOU_CALLBACK_FLAGS", "alarm,deviceStatus").strip()
IMOU_BASEPUSH = os.getenv("IMOU_BASEPUSH", "2").strip()  # per Imou docs, default "2"
DATA_DIR = os.getenv("DATA_DIR", ".").strip()
DB_PATH = os.path.join(DATA_DIR, "imou_status.sqlite3")

# Optional: preconfigured device IDs (comma-separated)
IMOU_DEVICE_IDS = [d.strip() for d in os.getenv("IMOU_DEVICE_IDS", "").split(",") if d.strip()]

# -----------------------------
# Flask
# -----------------------------
app = Flask(__name__)

# -----------------------------
# DB helpers (SQLite)
# -----------------------------
def db_connect() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Better concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn

def db_init():
    conn = db_connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            device_name TEXT,
            status TEXT,
            channel_status_json TEXT,
            last_seen_utc TEXT,
            last_event_summary TEXT,
            updated_at_utc TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT,
            msg_type TEXT,
            summary TEXT,
            occur_time TEXT,
            received_at_utc TEXT,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS callback_inbox (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              received_at_utc TEXT,
              headers_json TEXT,
              body_text TEXT
        );

        """
    )
    conn.commit()
    conn.close()

db_init()

def kv_get(key: str):
    conn = db_connect()
    row = conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
    conn.close()
    return None if not row else row["v"]

def kv_set(key: str, value: str):
    conn = db_connect()
    conn.execute(
        "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    conn.commit()
    conn.close()

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def upsert_device(device_id: str, **fields):
    keys = []
    vals = []
    for k, v in fields.items():
        keys.append(k)
        vals.append(v)
    keys.append("updated_at_utc")
    vals.append(now_utc_iso())

    conn = db_connect()
    existing = conn.execute("SELECT device_id FROM devices WHERE device_id=?", (device_id,)).fetchone()
    if existing:
        sets = ", ".join([f"{k}=?" for k in keys])
        conn.execute(f"UPDATE devices SET {sets} WHERE device_id=?", (*vals, device_id))
    else:
        cols = ", ".join(["device_id"] + keys)
        qmarks = ", ".join(["?"] * (1 + len(keys)))
        conn.execute(f"INSERT INTO devices({cols}) VALUES({qmarks})", (device_id, *vals))
    conn.commit()
    conn.close()

def add_event(device_id: str, msg_type: str, summary: str, occur_time: str, raw: dict):
    conn = db_connect()
    conn.execute(
        """
        INSERT INTO events(device_id,msg_type,summary,occur_time,received_at_utc,raw_json)
        VALUES(?,?,?,?,?,?)
        """,
        (device_id, msg_type, summary, occur_time, now_utc_iso(), json.dumps(raw, ensure_ascii=False)),
    )
    # keep only last 5000 events
    conn.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 5000)")
    conn.commit()
    conn.close()

def get_devices():
    conn = db_connect()
    rows = conn.execute(
        """
        SELECT device_id, device_name, status, channel_status_json, last_seen_utc, last_event_summary, updated_at_utc
        FROM devices
        ORDER BY COALESCE(device_name, device_id)
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_recent_events(limit=50):
    conn = db_connect()
    rows = conn.execute(
        """
        SELECT device_id, msg_type, summary, occur_time, received_at_utc
        FROM events
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def save_callback_inbox(headers: dict, body_text: str):
    conn = db_connect()
    conn.execute(
        "INSERT INTO callback_inbox(received_at_utc, headers_json, body_text) VALUES(?,?,?)",
        (now_utc_iso(), json.dumps(headers, ensure_ascii=False), body_text),
    )
    # тримаємо останні 200 записів
    conn.execute("DELETE FROM callback_inbox WHERE id NOT IN (SELECT id FROM callback_inbox ORDER BY id DESC LIMIT 200)")
    conn.commit()
    conn.close()


# -----------------------------
# Imou Open Platform client
# -----------------------------
def imou_base_url() -> str:
    if not IMOU_DATACENTER:
        raise RuntimeError("IMOU_DATACENTER is not set")
    return f"https://openapi-{IMOU_DATACENTER}.easy4ip.com/openapi"

def imou_sign(app_secret: str, ts: int, nonce: str) -> str:
    # Per Imou "Development Specification": md5("time:...,nonce:...,appSecret:...") lower-hex :contentReference[oaicite:3]{index=3}
    s = f"time:{ts},nonce:{nonce},appSecret:{app_secret}"
    return hashlib.md5(s.encode("utf-8")).hexdigest().lower()

def imou_post(endpoint: str, params: dict) -> dict:
    if not IMOU_APP_ID or not IMOU_APP_SECRET:
        raise RuntimeError("IMOU_APP_ID / IMOU_APP_SECRET not set")

    ts = int(time.time())
    nonce = str(uuid.uuid4())
    payload = {
        "system": {
            "ver": "1.0",
            "appId": IMOU_APP_ID,
            "sign": imou_sign(IMOU_APP_SECRET, ts, nonce),
            "time": ts,
            "nonce": nonce,
        },
        "id": str(uuid.uuid4()),
        "params": params or {},
    }

    url = f"{imou_base_url().rstrip('/')}/{endpoint.lstrip('/')}"
    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()

    # Standard format: { "result": { "code":"0", "msg":"...", "data":{...}}, "id":"..." }
    result = data.get("result", {})
    code = str(result.get("code", ""))
    if code != "0":
        raise RuntimeError(f"Imou API error {code}: {result.get('msg')}")
    return result.get("data", {}) or {}

def imou_get_admin_token() -> str:
    cached = kv_get("imou_access_token_json")
    if cached:
        try:
            obj = json.loads(cached)
            if obj.get("token") and obj.get("expires_at", 0) > time.time() + 60:
                return obj["token"]
        except Exception:
            pass

    # accessToken API: no params required :contentReference[oaicite:4]{index=4}
    data = imou_post("accessToken", {})
    token = data["accessToken"]
    expire_sec = int(data.get("expireTime", 0))
    expires_at = int(time.time()) + max(0, expire_sec) - 600  # refresh ~10 min early

    kv_set("imou_access_token_json", json.dumps({"token": token, "expires_at": expires_at}))
    return token

def imou_set_message_callback(callback_url: str, status: str = "on"):
    token = imou_get_admin_token()
    # setMessageCallback requires token, status, callbackUrl, callbackFlag :contentReference[oaicite:5]{index=5}
    params = {
        "token": token,
        "status": status,
        "callbackUrl": callback_url if status == "on" else "",
        "callbackFlag": IMOU_CALLBACK_FLAGS if status == "on" else "",
        "basePush": IMOU_BASEPUSH,
    }
    imou_post("setMessageCallback", params)

def imou_device_online(device_id: str) -> dict:
    token = imou_get_admin_token()
    # deviceOnline returns onLine = 0/1/3/4 and channels[] :contentReference[oaicite:6]{index=6}
    return imou_post("deviceOnline", {"token": token, "deviceId": device_id})

def imou_list_device_details_by_ids(device_ids: list[str]) -> list[dict]:
    token = imou_get_admin_token()
    # listDeviceDetailsByIds expects deviceList with channelId list :contentReference[oaicite:7]{index=7}
    payload_list = [{"deviceId": d, "channelId": ["0"]} for d in device_ids]
    data = imou_post("listDeviceDetailsByIds", {"token": token, "deviceList": payload_list})
    return data.get("deviceList", []) or []

def imou_get_message_callback():
    token = imou_get_admin_token()
    return imou_post("getMessageCallback", {"token": token})

# -----------------------------
# Admin protection
# -----------------------------
def require_admin():
    if not ADMIN_KEY:
        abort(500, description="ADMIN_KEY is not configured")
    key = request.headers.get("X-Admin-Key", "") or request.args.get("key", "")
    if key != ADMIN_KEY:
        abort(401)

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return "ok", 200

@app.get("/admin/get-callback")
def admin_get_callback():
    require_admin()
    return jsonify(imou_get_message_callback())

@app.get("/imou/callback")
def imou_callback_health():
    return "callback alive", 200



@app.get("/api/status")
def api_status():
    return jsonify(
        {
            "callback_endpoint": callback_endpoint(),
            "devices": get_devices(),
            "recent_events": get_recent_events(50),
        }
    )

def callback_endpoint() -> str:
    base = PUBLIC_BASE_URL
    if not base:
        # Best effort: derive from request if available
        try:
            base = request.url_root.rstrip("/")
        except Exception:
            base = ""
    return f"{base}/imou/callback" if base else "/imou/callback"


@app.get("/admin/last-callbacks")
def admin_last_callbacks():
    require_admin()
    conn = db_connect()
    rows = conn.execute(
        "SELECT id, received_at_utc, body_text FROM callback_inbox ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.get("/")
def index():
    devices = get_devices()
    events = get_recent_events(30)
    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Imou Cameras Status</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 20px; }
    .row { display: flex; gap: 16px; flex-wrap: wrap; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 14px; min-width: 280px; flex: 1; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #eee; padding: 10px 8px; text-align: left; vertical-align: top; }
    th { background: #fafafa; }
    .pill { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; border: 1px solid #ddd; }
    .ok { background: #eaffea; }
    .bad { background: #ffecec; }
    code { background: #f6f6f6; padding: 2px 6px; border-radius: 6px; }
    .muted { color: #666; font-size: 12px; }
    button { padding: 8px 12px; border-radius: 10px; border: 1px solid #ddd; background: #fff; cursor: pointer; }
    button:hover { background: #fafafa; }
    input { padding: 8px 10px; border-radius: 10px; border: 1px solid #ddd; width: 360px; max-width: 100%; }
  </style>
</head>
<body>
  <h2>Imou Cameras Status</h2>

  <div class="row">
    <div class="card">
      <div><b>Message Callback Address</b></div>
      <div class="muted">Set this URL in Imou (or call <code>/admin/set-callback</code>):</div>
      <div style="margin-top:8px;"><code id="cb">{{ cb }}</code></div>
      <div class="muted" style="margin-top:8px;">Your callback must return HTTP 200 for Imou to keep pushing events.</div>
    </div>

    <div class="card">
      <div><b>Admin tools</b> <span class="muted">(requires ADMIN_KEY)</span></div>
      <div style="margin-top:10px;">
        <div class="muted">Sync device details (optional):</div>
        <button onclick="adminPost('/admin/sync')">Sync now</button>
      </div>
      <div style="margin-top:10px;">
        <div class="muted">Set Imou callback URL (optional):</div>
        <input id="cburl" value="{{ cb }}" />
        <button onclick="adminPost('/admin/set-callback', {callback_url: document.getElementById('cburl').value})">Set callback</button>
      </div>
      <div class="muted" style="margin-top:10px;">
        Tip: pass admin key header <code>X-Admin-Key</code> (handled by the JS button).
      </div>
    </div>
  </div>

  <h3 style="margin-top:18px;">Devices</h3>
  <table>
    <thead>
      <tr>
        <th>Name</th>
        <th>Device ID</th>
        <th>Status</th>
        <th>Last seen (UTC)</th>
        <th>Last event</th>
      </tr>
    </thead>
    <tbody id="devrows">
      {% for d in devices %}
      <tr>
        <td>{{ d.device_name or "" }}</td>
        <td><code>{{ d.device_id }}</code></td>
        <td>
          {% set st = (d.status or "unknown") %}
          <span class="pill {{ 'ok' if st in ['online','1'] else ('bad' if st in ['offline','0'] else '') }}">{{ st }}</span>
        </td>
        <td class="muted">{{ d.last_seen_utc or "" }}</td>
        <td class="muted">{{ d.last_event_summary or "" }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>

  <h3 style="margin-top:18px;">Recent events</h3>
  <table>
    <thead>
      <tr>
        <th>Device</th>
        <th>Type</th>
        <th>Summary</th>
        <th>Occur time</th>
        <th>Received (UTC)</th>
      </tr>
    </thead>
    <tbody>
      {% for e in events %}
      <tr>
        <td><code>{{ e.device_id }}</code></td>
        <td>{{ e.msg_type }}</td>
        <td class="muted">{{ e.summary }}</td>
        <td class="muted">{{ e.occur_time }}</td>
        <td class="muted">{{ e.received_at_utc }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>

<script>
  const ADMIN_KEY = "{{ admin_key_present }}";

  async function adminPost(path, body) {
    if (!ADMIN_KEY) {
      alert("ADMIN_KEY is not set on server.");
      return;
    }
    const key = prompt("Enter ADMIN_KEY:");
    if (!key) return;

    const r = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Admin-Key": key },
      body: body ? JSON.stringify(body) : "{}"
    });
    const t = await r.text();
    if (!r.ok) alert("Error: " + t);
    else location.reload();
  }

  // Simple auto-refresh every 10s
  setInterval(async () => {
    try {
      const r = await fetch("/api/status");
      if (!r.ok) return;
      const data = await r.json();
      const tbody = document.getElementById("devrows");
      tbody.innerHTML = "";
      (data.devices || []).forEach(d => {
        const st = d.status || "unknown";
        const ok = (st === "online" || st === "1");
        const bad = (st === "offline" || st === "0");
        const cls = ok ? "ok" : (bad ? "bad" : "");
        tbody.innerHTML += `
          <tr>
            <td>${d.device_name || ""}</td>
            <td><code>${d.device_id}</code></td>
            <td><span class="pill ${cls}">${st}</span></td>
            <td class="muted">${d.last_seen_utc || ""}</td>
            <td class="muted">${d.last_event_summary || ""}</td>
          </tr>`;
      });
      document.getElementById("cb").textContent = data.callback_endpoint || "";
    } catch (e) {}
  }, 10000);
</script>

</body>
</html>
        """,
        devices=devices,
        events=events,
        cb=callback_endpoint(),
        admin_key_present=("yes" if ADMIN_KEY else ""),
    )

@app.post("/imou/callback")
def imou_callback():
    raw_text = request.get_data(as_text=True) or ""
    save_callback_inbox(dict(request.headers), raw_text)

    # завжди 200
    try:
        raw = request.get_json(silent=True)
        if raw is None:
            raw = json.loads(raw_text or "{}")

        # інколи корисні дані в params
        if isinstance(raw, dict) and isinstance(raw.get("params"), dict):
            raw = raw["params"]

        messages = raw if isinstance(raw, list) else [raw]

        for msg in messages:
            if not isinstance(msg, dict):
                # все одно збережемо як event
                add_event("__unknown__", "raw", "non-dict payload", "", {"raw": raw_text})
                continue

            device_id = (
                (msg.get("deviceId") or "").strip()
                or (msg.get("did") or "").strip()
                or "__unknown__"
            )
            msg_type = (msg.get("msgType") or msg.get("type") or "unknown").strip()
            occur_time = str(msg.get("occurTime") or msg.get("time") or "")

            # нормалізація статусу
            status = ""
            if msg_type in ("online", "offline"):
                status = msg_type
                summary = f"deviceStatus: {status}"
            elif msg_type == "deviceStatus":
                status = (msg.get("status") or "").strip().lower()
                summary = f"deviceStatus: {status or 'unknown'}"
            elif msg_type == "alarm":
                summary = f"alarm: {(msg.get('alarmName') or '')} {(msg.get('alarmType') or '')}".strip()
            else:
                summary = msg_type

            add_event(device_id, msg_type, summary, occur_time, msg)
            upsert_device(device_id, status=status, last_seen_utc=now_utc_iso(), last_event_summary=summary)

    except Exception:
        app.logger.exception("IMOU CALLBACK processing error")

    return "OK", 200


# -----------------------------
# Admin endpoints
# -----------------------------
@app.post("/admin/set-callback")
def admin_set_callback():
    require_admin()
    body = request.get_json(silent=True) or {}
    cb = (body.get("callback_url") or callback_endpoint()).strip()
    if not cb.startswith("http"):
        abort(400, description="callback_url must be absolute (https://...)")

    imou_set_message_callback(cb, status="on")
    return jsonify({"ok": True, "callback_url": cb, "flags": IMOU_CALLBACK_FLAGS})

@app.post("/admin/sync")
def admin_sync():
    require_admin()

    if not IMOU_DEVICE_IDS:
        # If you want auto-discovery, you can add listDeviceDetailsByPage/deviceBaseList later.
        # For now, sync requires IMOU_DEVICE_IDS env var.
        abort(400, description="Set IMOU_DEVICE_IDS (comma-separated) to use /admin/sync")

    details = imou_list_device_details_by_ids(IMOU_DEVICE_IDS)
    for d in details:
        device_id = str(d.get("deviceId", "")).strip()
        if not device_id:
            continue

        device_name = d.get("deviceName") or ""
        device_status = d.get("deviceStatus") or "unknown"  # online/offline/sleep/upgrading :contentReference[oaicite:13]{index=13}

        channel_list = d.get("channelList") or []
        channel_status = {}
        for ch in channel_list:
            cid = str(ch.get("channelId"))
            channel_status[cid] = ch.get("channelStatus") or ""

        upsert_device(
            device_id,
            device_name=device_name,
            status=device_status,
            channel_status_json=json.dumps(channel_status, ensure_ascii=False),
            last_seen_utc=now_utc_iso(),
        )

        # Also refresh real-time online numeric status (optional)
        try:
            online = imou_device_online(device_id)  # returns onLine=0/1/3/4 :contentReference[oaicite:14]{index=14}
            upsert_device(
                device_id,
                status=str(online.get("onLine", device_status)),
                channel_status_json=json.dumps(online.get("channels", []), ensure_ascii=False),
            )
        except Exception:
            pass

    return jsonify({"ok": True, "synced": len(details)})

# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
