import os
import json
import time
import uuid
import hashlib
import sqlite3
import base64
import math
import re
import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify, abort, render_template_string

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build


# -----------------------------
# Helpers
# -----------------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


KYIV_TZ = ZoneInfo("Europe/Kyiv")

def now_kyiv_iso() -> str:
    # ISO 8601 with correct +02:00/+03:00 depending on DST
    return datetime.now(KYIV_TZ).replace(microsecond=0).isoformat()

def now_utc_iso() -> str:
    # Keep UTC helper in case you need it elsewhere
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()



def _parse_iso_dt(s: str):
    """Parse ISO8601 string to datetime (aware if offset present). Returns None on failure."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _format_hhmm(total_seconds: int) -> str:
    """Formats seconds as '<H> год <MM> хв'. Always includes hours and minutes."""
    total_seconds = max(0, int(total_seconds or 0))
    minutes = total_seconds // 60
    h = minutes // 60
    m = minutes % 60
    return f"{h} год {m:02d} хв"


def _build_power_interval_note(new_status: str, prev_changed_at_iso: str, now_iso: str) -> str:
    """
    new_status: 'online' or 'offline'
    prev_changed_at_iso: when the previous status started (Kyiv ISO string)
    now_iso: current time (Kyiv ISO string)

    Returns a Ukrainian note, e.g. 'Було без світла: 1 год 23 хв'
    """
    dt0 = _parse_iso_dt(prev_changed_at_iso)
    dt1 = _parse_iso_dt(now_iso)
    if not dt0 or not dt1:
        return ""
    delta = int((dt1 - dt0).total_seconds())
    if delta < 0:
        return ""

    # If we are going ONLINE now, we were OFFLINE before -> "without power" interval.
    # If we are going OFFLINE now, we were ONLINE before -> "with power" interval.
    label = "Було без світла" if new_status == "online" else "Було зі світлом"
    return f"{label}: {_format_hhmm(delta)}"


# -----------------------------
# Config (Railway env vars)
# -----------------------------
IMOU_DATACENTER = os.getenv("IMOU_DATACENTER", "").strip()  # e.g. "fk" for Central Europe
IMOU_APP_ID = os.getenv("IMOU_APP_ID", "").strip()
IMOU_APP_SECRET = os.getenv("IMOU_APP_SECRET", "").strip()

ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")

IMOU_CALLBACK_FLAGS = os.getenv("IMOU_CALLBACK_FLAGS", "alarm,deviceStatus").strip()
IMOU_BASEPUSH = os.getenv("IMOU_BASEPUSH", "2").strip()

DATA_DIR = os.getenv("DATA_DIR", ".").strip()
DB_PATH = os.path.join(DATA_DIR, "imou_status.sqlite3")

IMOU_DEVICE_IDS = [d.strip() for d in os.getenv("IMOU_DEVICE_IDS", "").split(",") if d.strip()]


# -----------------------------
# Telegram notifications
# -----------------------------
TELEGRAM_ENABLED = env_bool("TELEGRAM_ENABLED", False)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()  # channel username (@xxx) or numeric (-100...)
TELEGRAM_TIMEOUT_SEC = int(os.getenv("TELEGRAM_TIMEOUT_SEC", "10"))
# Default: parking device requested by user
TELEGRAM_PARKING_DEVICE_ID = os.getenv("TELEGRAM_PARKING_DEVICE_ID", "14062AEPBV3882A").strip()
TELEGRAM_PARKING_DEVICE_NAME = os.getenv("TELEGRAM_PARKING_DEVICE_NAME", "Парковка").strip()


# -----------------------------
# DTEK forecast (optional)
# -----------------------------
# NOTE:
# - Telegram Bot API cannot read messages from other bots.
# - To pull data from @DTEKKyivskielectromerezhibot automatically, we use a Telegram *user* session via Telethon (MTProto).
# - This feature is OPTIONAL and does nothing unless DTEK_FORECAST_ENABLED=1 and Telethon creds are provided.
DTEK_FORECAST_ENABLED = env_bool("DTEK_FORECAST_ENABLED", False)
DTEK_BOT_USERNAME = os.getenv("DTEK_BOT_USERNAME", "DTEKKyivskielectromerezhibot").strip().lstrip("@")

# Telethon (MTProto user session) credentials
DTEK_TG_API_ID = os.getenv("DTEK_TG_API_ID", "").strip()
DTEK_TG_API_HASH = os.getenv("DTEK_TG_API_HASH", "").strip()
# Preferred: StringSession (single env var, no local session file needed)
DTEK_TG_SESSION = os.getenv("DTEK_TG_SESSION", "").strip()
# Alternative: session file path (should be on a persistent volume if you use it)
DTEK_TG_SESSION_FILE = os.getenv("DTEK_TG_SESSION_FILE", os.path.join(DATA_DIR, "dtek_user.session")).strip()

# Menu navigation inside DTEK bot (pipe-separated). Default reflects what DTEK announced publicly.
# You may need to adjust if DTEK changes labels.
DTEK_MENU_SEQUENCE = os.getenv("DTEK_MENU_SEQUENCE", "Можливі відключення").strip()
DTEK_FETCH_TIMEOUT_SEC = int(os.getenv("DTEK_FETCH_TIMEOUT_SEC", "20"))
DTEK_FORECAST_CACHE_SEC = int(os.getenv("DTEK_FORECAST_CACHE_SEC", "300"))
DTEK_ATTACH_TO_OFFLINE_ALERT = env_bool("DTEK_ATTACH_TO_OFFLINE_ALERT", True)


# Debug: store raw callback payloads into callback_inbox (keep last 200)
DEBUG_CALLBACK_INBOX = env_bool("DEBUG_CALLBACK_INBOX", False)
CALLBACK_INBOX_MAX = int(os.getenv("CALLBACK_INBOX_MAX", "200"))
CALLBACK_INBOX_MAX_BODY_CHARS = int(os.getenv("CALLBACK_INBOX_MAX_BODY_CHARS", "50000"))

# -----------------------------
# Google Drive / Sheets config
# -----------------------------
# Auth, як у example.py: base64 service-account json
GDRIVE_SA_JSON_B64 = os.getenv("GDRIVE_SA_JSON_B64", "").strip()

# ДЕ лежить папка imou_project:
# 1) найкраще: одразу ID папки imou_project
GDRIVE_IMOU_PROJECT_FOLDER_ID = os.getenv("GDRIVE_IMOU_PROJECT_FOLDER_ID", "").strip()
# 2) або ID "root parent", де ми створимо/знайдемо папку "imou_project"
GDRIVE_ROOT_FOLDER_ID = os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()

# Spreadsheet
GDRIVE_EVENTS_SPREADSHEET_ID = os.getenv("GDRIVE_EVENTS_SPREADSHEET_ID", "").strip()
GDRIVE_EVENTS_SPREADSHEET_NAME = os.getenv("GDRIVE_EVENTS_SPREADSHEET_NAME", "imou_events").strip()
GDRIVE_EVENTS_TAB_NAME = os.getenv("GDRIVE_EVENTS_TAB_NAME", "Events").strip()

# batching/throttle
GDRIVE_EVENTS_APPEND_BATCH = int(os.getenv("GDRIVE_EVENTS_APPEND_BATCH", "50"))
GDRIVE_FLUSH_INTERVAL_SEC = int(os.getenv("GDRIVE_FLUSH_INTERVAL_SEC", "5"))
GDRIVE_EVENTS_ENABLED = env_bool("GDRIVE_EVENTS_ENABLED", True)

# -----------------------------
# Flask
# -----------------------------
app = Flask(__name__)


# -----------------------------
# Telegram helpers (best-effort)
# -----------------------------
def _normalize_status(v) -> str:
    s = ("" if v is None else str(v)).strip().lower()
    if s in ("1", "true", "yes", "y", "on", "online"):
        return "online"
    if s in ("0", "false", "no", "n", "off", "offline"):
        return "offline"
    return s


def telegram_enabled() -> bool:
    return bool(TELEGRAM_ENABLED and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def telegram_send_message(text: str) -> dict:
    """Send a plain-text message to Telegram channel/chat. Never raises (best-effort)."""
    if not telegram_enabled():
        return {"ok": False, "reason": "telegram disabled or missing env"}
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }
        r = requests.post(url, json=payload, timeout=max(3, int(TELEGRAM_TIMEOUT_SEC or 10)))
        if r.status_code >= 400:
            return {"ok": False, "status": r.status_code, "error": r.text[:500]}
        data = r.json()
        return data if isinstance(data, dict) else {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def maybe_notify_telegram_device_status(device_id: str, status: str, interval_note: str = ""):
    """Sends ONLY for the configured parking device, and only for online/offline."""
    st = _normalize_status(status)
    if not device_id or device_id == "__unknown__":
        return
    if device_id != TELEGRAM_PARKING_DEVICE_ID:
        return
    if st not in ("online", "offline"):
        return

    emoji = "🟢" if st == "online" else "🔴"
    base = f"{emoji} ДАЛИ СВІТЛО" if st == "online" else f"{emoji} ВІДКЛЮЧИЛИ СВІТЛО"

    text = base
    if interval_note:
        text = f"{base}\n{interval_note}"

    # Optional: attach DTEK forecast to OFFLINE alerts
    if st == "offline" and DTEK_ATTACH_TO_OFFLINE_ALERT:
        try:
            fc = dtek_get_forecast_cached()
            if isinstance(fc, dict):
                restore = (fc.get("restore_at_kyiv") or "").strip()
                note = (fc.get("note") or "").strip()
                if restore:
                    text += f"\n⏱️ Прогноз включення (ДТЕК): {restore}"
                elif note and (fc.get("ok") or note == "відключень не планується"):
                    text += f"\n⏱️ ДТЕК: {note}"
        except Exception as e:
            app.logger.warning(f"DTEK forecast failed: {e}")

    res = telegram_send_message(text)

    if not res.get("ok"):
        # keep quiet, but log to app logger
        app.logger.warning(f"Telegram send failed: {res}")


# -----------------------------
# DB helpers (SQLite)
# -----------------------------
def db_connect() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
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
            status_changed_at_kyiv TEXT,
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

        -- Queue for Google Sheets (stores ALL events independently from events retention)
        CREATE TABLE IF NOT EXISTS sheet_queue (
            uid TEXT PRIMARY KEY,
            row_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0,
            sent_at_utc TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sheet_queue_sent_created
            ON sheet_queue(sent, created_at_utc);
        """
    )
    conn.commit()
    conn.close()


def db_migrate():
    """Lightweight DB migration to keep existing deployments working."""
    conn = db_connect()
    try:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(devices)").fetchall()]
        if "status_changed_at_kyiv" not in cols:
            conn.execute("ALTER TABLE devices ADD COLUMN status_changed_at_kyiv TEXT")
        conn.commit()
    finally:
        conn.close()


db_init()
db_migrate()


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



# -----------------------------
# DTEK forecast via Telegram (Telethon user session)
# -----------------------------
_DTEK_CACHE_KEY = "dtek_forecast_cache_v1"


def _dtek_feature_ready() -> tuple[bool, str]:
    if not DTEK_FORECAST_ENABLED:
        return False, "DTEK_FORECAST_ENABLED=0"
    if not (DTEK_TG_API_ID and DTEK_TG_API_HASH):
        return False, "Missing DTEK_TG_API_ID / DTEK_TG_API_HASH"
    if not (DTEK_TG_SESSION or DTEK_TG_SESSION_FILE):
        return False, "Missing DTEK_TG_SESSION or DTEK_TG_SESSION_FILE"
    return True, ""


def _dtek_cache_load() -> dict:
    try:
        raw = kv_get(_DTEK_CACHE_KEY)
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _dtek_cache_save(obj: dict):
    try:
        kv_set(_DTEK_CACHE_KEY, json.dumps(obj, ensure_ascii=False))
    except Exception:
        pass


def _dtek_format_dt(dt: datetime) -> str:
    dt = dt.astimezone(KYIV_TZ)
    return dt.strftime("%d.%m.%Y %H:%M")


def _dtek_parse_restore_from_text(text: str) -> dict:
    """
    Best-effort parser for DTEK bot messages.
    Returns:
      {
        ok: bool,
        restore_dt_iso: str|None,    # Kyiv timezone ISO
        restore_at_kyiv: str|None,   # formatted: HH:MM (dd.mm)
        note: str,                  # fallback / explanation
        raw: str                    # clipped original text
      }
    """
    now_dt = datetime.now(KYIV_TZ)
    t = (text or "").strip()
    t_norm = " ".join(t.split())
    raw_clip = t[:4000]

    if not t_norm:
        return {"ok": False, "note": "empty response", "raw": raw_clip}

    # If DTEK explicitly says there are no outages
    if re.search(r"(відключень\s+не\s+буде|не\s+планується\s+відключень|відключення\s+не\s+передбачені)", t_norm, re.IGNORECASE):
        return {"ok": True, "restore_dt_iso": None, "restore_at_kyiv": None, "note": "відключень не планується", "raw": raw_clip}

    # 0) Most reliable: explicit phrase used by DTEK bot
    # Example (from your screenshot):
    #   "Орієнтовний час відновлення електроенергії: 03.02.2026 21:00."
    # NOTE: This must take priority, because the same message may also contain "Час початку: ...",
    # and a generic date+time regex would otherwise pick the *start* time by accident.
    for rx in [
        r"(?:орієнтовн\w*\s+час\s+відновлення\s+електроенергії\s*[:\-]?\s*)(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\s+(\d{1,2})[:.](\d{2})",
        r"(?:орієнтовн\w*\s+час\s+відновлення\s*[:\-]?\s*)(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\s+(\d{1,2})[:.](\d{2})",
        r"(?:очікуван\w*\s+час\s+відновлення\s*(?:електроенергії)?\s*[:\-]?\s*)(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\s+(\d{1,2})[:.](\d{2})",
    ]:
        m0 = re.search(rx, t_norm, re.IGNORECASE)
        if m0:
            dd, mo, yy, hh, mm = m0.group(1), m0.group(2), m0.group(3), m0.group(4), m0.group(5)
            dd, mo = int(dd), int(mo)
            yy = int(yy) if yy else now_dt.year
            if yy < 100:
                yy += 2000
            dt = datetime(yy, mo, dd, int(hh), int(mm), tzinfo=KYIV_TZ)
            return {"ok": True, "restore_dt_iso": dt.isoformat(), "restore_at_kyiv": _dtek_format_dt(dt), "note": "", "raw": raw_clip}

    # 1) Try any date+time occurrences, but prefer those close to "відновлення/включення" keywords.
    # This avoids accidentally picking the *start* time when both start & restore are present.
    dt_candidates = []

    def _score_ctx(pos: int) -> int:
        ctx = t_norm[max(0, pos - 90):pos].casefold()
        score = 0
        if ("віднов" in ctx) or ("включ" in ctx):
            score += 10
        if ("орієнтов" in ctx) or ("очікуван" in ctx):
            score += 5
        if ("почат" in ctx):
            score -= 10
        return score

    # Pattern A: date then time
    for m1 in re.finditer(r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\s*(?:о\s*)?(\d{1,2})[:.](\d{2})", t_norm):
        dd, mo, yy, hh, mm = m1.group(1), m1.group(2), m1.group(3), m1.group(4), m1.group(5)
        try:
            dd, mo = int(dd), int(mo)
            hh, mm = int(hh), int(mm)
            if not (1 <= dd <= 31 and 1 <= mo <= 12 and 0 <= hh <= 23 and 0 <= mm <= 59):
                continue
            yy = int(yy) if yy else now_dt.year
            if yy < 100:
                yy += 2000
        except Exception:
            continue
        dt_candidates.append((_score_ctx(m1.start()), m1.start(), yy, mo, dd, hh, mm))

    # Pattern B: time then date
    for m1 in re.finditer(r"(\d{1,2})[:.](\d{2})\s*(?:,?\s*|о\s*)(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?", t_norm):
        hh, mm, dd, mo, yy = m1.group(1), m1.group(2), m1.group(3), m1.group(4), m1.group(5)
        try:
            dd, mo = int(dd), int(mo)
            hh, mm = int(hh), int(mm)
            if not (1 <= dd <= 31 and 1 <= mo <= 12 and 0 <= hh <= 23 and 0 <= mm <= 59):
                continue
            yy = int(yy) if yy else now_dt.year
            if yy < 100:
                yy += 2000
        except Exception:
            continue
        dt_candidates.append((_score_ctx(m1.start()), m1.start(), yy, mo, dd, hh, mm))

    if dt_candidates:
        # higher score first, then earlier in text
        dt_candidates.sort(key=lambda x: (-x[0], x[1]))
        _, _, yy, mo, dd, hh, mm = dt_candidates[0]
        dt = datetime(yy, mo, dd, hh, mm, tzinfo=KYIV_TZ)
        return {"ok": True, "restore_dt_iso": dt.isoformat(), "restore_at_kyiv": _dtek_format_dt(dt), "note": "", "raw": raw_clip}

    # 2) Try typical phrase with only time:
    # Examples: 'до 18:30', 'очікуваний час відновлення 18:30'
    # We prefer matches that appear close to keywords.
    time_candidates = []

    for rx in [
        r"(?:очікуван\w*\s+час\s+(?:відновлення|включення)[^\d]{0,20})(\d{1,2})[:.](\d{2})",
        r"(?:відновлен\w*[^\d]{0,20})(\d{1,2})[:.](\d{2})",
        r"(?:включен\w*[^\d]{0,20})(\d{1,2})[:.](\d{2})",
        r"\bдо\s*(\d{1,2})[:.](\d{2})\b",
    ]:
        for m in re.finditer(rx, t_norm, re.IGNORECASE):
            hh, mm = int(m.group(1)), int(m.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                time_candidates.append((m.start(), hh, mm, rx))

    if time_candidates:
        # earliest keyword match
        time_candidates.sort(key=lambda x: x[0])
        _, hh, mm, _ = time_candidates[0]
        dt = datetime(now_dt.year, now_dt.month, now_dt.day, hh, mm, tzinfo=KYIV_TZ)
        # If time already passed significantly, assume next day
        if dt < now_dt - timedelta(minutes=30):
            dt = dt + timedelta(days=1)
        return {"ok": True, "restore_dt_iso": dt.isoformat(), "restore_at_kyiv": _dtek_format_dt(dt), "note": "", "raw": raw_clip}

    # 3) Fallback: no parse
    return {"ok": False, "note": "не вдалося розпізнати прогноз часу включення з відповіді ДТЕК", "raw": raw_clip}


async def _dtek_fetch_text_via_telethon() -> dict:
    """
    Fetch last DTEK bot responses and try to navigate the menu to reach the outage schedule.

    Notes:
      - DTEK bot menu in your screenshot is a *reply keyboard* (not inline).
        In practice, pressing such buttons == sending the exact button text.
      - We try both:
          (a) click button from reply_markup when present (more robust for emojis),
          (b) send text fallback.
    """
    if not DTEK_FORECAST_ENABLED:
        return {"ok": False, "error": "DTEK_FORECAST_ENABLED is disabled"}

    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except Exception as e:
        return {"ok": False, "error": f"telethon import failed: {e}"}

    # Validate creds early
    try:
        api_id = int(DTEK_TG_API_ID)
    except Exception:
        return {"ok": False, "error": "DTEK_TG_API_ID must be an integer"}
    api_hash = DTEK_TG_API_HASH
    if not api_hash:
        return {"ok": False, "error": "DTEK_TG_API_HASH is empty"}

    session = StringSession(DTEK_TG_SESSION) if DTEK_TG_SESSION else DTEK_TG_SESSION_FILE

    # Steps: pipe-separated labels we want to press. For your menu:
    #   "Можливі відключення"
    # (If you want the schedule image instead, use "Графік відключень".)
    steps = [s.strip() for s in (DTEK_MENU_SEQUENCE or "").split("|") if s.strip()]

    def _iter_button_texts(msg):
        out = []
        btns = getattr(msg, "buttons", None)
        if not btns:
            return out
        try:
            for row in btns:
                for b in row:
                    t = getattr(b, "text", None)
                    if t:
                        out.append(str(t))
        except Exception:
            pass
        return out

    def _match_button(msg, needle: str):
        n = (needle or "").strip().casefold()
        if not n:
            return None
        for t in _iter_button_texts(msg):
            if n in t.casefold():
                return t
        return None

    try:
        async with TelegramClient(session, api_id, api_hash) as client:
            if not await client.is_user_authorized():
                return {
                    "ok": False,
                    "error": "Telethon session is not authorized. Create DTEK_TG_SESSION (StringSession) first.",
                }

            bot = await client.get_entity(DTEK_BOT_USERNAME)

            collected_texts = []

            # Conversation-based navigation (best-effort)
            try:
                async with client.conversation(bot, timeout=max(5, DTEK_FETCH_TIMEOUT_SEC)) as conv:
                    # Kick-start the bot to ensure keyboard is present.
                    await conv.send_message("/start")
                    msg = await conv.get_response()
                    if msg and (msg.raw_text or msg.message):
                        collected_texts.append(msg.raw_text or msg.message or "")

                    for step in steps:
                        # Try to press an on-screen button by substring match
                        btn_text = _match_button(msg, step) if msg else None

                        pressed = False
                        if msg and btn_text:
                            # Try true "click" first (works for inline keyboards too)
                            try:
                                await msg.click(text=btn_text)
                                pressed = True
                            except Exception:
                                pressed = False

                        if not pressed:
                            # Reply-keyboard fallback: send exact text (prefer matched button text if present)
                            await conv.send_message(btn_text or step)

                        # Wait for bot response and keep as current message (for next step keyboard)
                        msg = await conv.get_response()
                        if msg and (msg.raw_text or msg.message):
                            collected_texts.append(msg.raw_text or msg.message or "")

                    # Some bots send multiple messages; collect a few extra quickly
                    for _ in range(4):
                        try:
                            extra = await asyncio.wait_for(conv.get_response(), timeout=1.5)
                        except Exception:
                            break
                        if extra and (extra.raw_text or extra.message):
                            collected_texts.append(extra.raw_text or extra.message or "")
            except Exception as e:
                app.logger.warning(f"DTEK conversation flow failed, fallback to history: {e}")

            if not collected_texts:
                # Fallback: read a few last messages from history
                msgs = await client.get_messages(bot, limit=10)
                msgs = list(reversed(msgs))
                for m in msgs:
                    if m and (m.raw_text or m.message):
                        collected_texts.append(m.raw_text or m.message or "")

            text = "\n\n".join([t for t in collected_texts if (t or '').strip()]).strip()
            return {"ok": True, "text": text}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def dtek_get_forecast_cached(force: bool = False) -> dict:
    """
    Public wrapper used by Telegram alerts.
    Uses SQLite kv cache to avoid frequent calls to DTEK bot.
    """
    ok, reason = _dtek_feature_ready()
    if not ok:
        return {"ok": False, "note": reason}

    now_ts = time.time()
    cached = _dtek_cache_load()
    try:
        cached_ts = float(cached.get("ts_epoch") or 0.0)
    except Exception:
        cached_ts = 0.0

    if (not force) and cached and (now_ts - cached_ts) < max(30, int(DTEK_FORECAST_CACHE_SEC or 300)):
        return cached

    # Fetch + parse (best-effort)
    try:
        res = asyncio.run(asyncio.wait_for(_dtek_fetch_text_via_telethon(), timeout=max(6, DTEK_FETCH_TIMEOUT_SEC)))
    except RuntimeError:
        # If we're already in an event loop (rare in Flask), create a new loop
        loop = asyncio.new_event_loop()
        try:
            res = loop.run_until_complete(asyncio.wait_for(_dtek_fetch_text_via_telethon(), timeout=max(6, DTEK_FETCH_TIMEOUT_SEC)))
        finally:
            try:
                loop.close()
            except Exception:
                pass
    except Exception as e:
        res = {"ok": False, "error": str(e)}

    out = {
        "ok": False,
        "ts_epoch": now_ts,
        "ts_kyiv": now_kyiv_iso(),
        "restore_dt_iso": None,
        "restore_at_kyiv": None,
        "note": "",
        "raw": "",
        "error": "",
    }

    if not res.get("ok"):
        out["error"] = str(res.get("error") or "unknown error")
        out["note"] = "DTEK fetch failed"
        _dtek_cache_save(out)
        return out

    parsed = _dtek_parse_restore_from_text(res.get("text") or "")
    out.update(parsed)
    out["ok"] = bool(parsed.get("ok"))
    out["ts_epoch"] = now_ts
    out["ts_kyiv"] = now_kyiv_iso()
    _dtek_cache_save(out)
    return out

def upsert_device(device_id: str, **fields):
    keys = []
    vals = []
    for k, v in fields.items():
        keys.append(k)
        vals.append(v)
    keys.append("updated_at_utc")
    vals.append(now_kyiv_iso())

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


def get_device_name(device_id: str) -> str:
    conn = db_connect()
    row = conn.execute("SELECT device_name FROM devices WHERE device_id=?", (device_id,)).fetchone()
    conn.close()
    if not row:
        return ""
    return (row["device_name"] or "").strip()


def get_device_status(device_id: str) -> str:
    conn = db_connect()
    row = conn.execute("SELECT status FROM devices WHERE device_id=?", (device_id,)).fetchone()
    conn.close()
    return "" if not row else (row["status"] or "")



def get_device_status_info(device_id: str) -> tuple[str, str]:
    """Returns (status, status_changed_at_kyiv). Empty strings if missing."""
    conn = db_connect()
    row = conn.execute(
        "SELECT status, COALESCE(status_changed_at_kyiv, '') AS status_changed_at_kyiv FROM devices WHERE device_id=?",
        (device_id,),
    ).fetchone()
    conn.close()
    if not row:
        return "", ""
    return (row["status"] or ""), (row["status_changed_at_kyiv"] or "")



def add_event(device_id: str, msg_type: str, summary: str, occur_time: str, raw: dict):
    """
    1) Store in SQLite (keep last 5000)
    2) Store in sheet_queue (keeps ALL events)
    3) Try flush to Google Sheets (best-effort)
    """
    received_at = now_kyiv_iso()
    raw_json = json.dumps(raw, ensure_ascii=False)

    # ---- (A) SQLite events (keeps only last 5000) ----
    conn = db_connect()
    conn.execute(
        """
        INSERT INTO events(device_id,msg_type,summary,occur_time,received_at_utc,raw_json)
        VALUES(?,?,?,?,?,?)
        """,
        (device_id, msg_type, summary, occur_time, received_at, raw_json),
    )
    conn.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 5000)")
    conn.commit()
    conn.close()

    # ---- (B) Google Sheets queue (keeps ALL) ----
    enqueue_event_for_sheets(
        device_id=device_id,
        device_name=get_device_name(device_id),
        msg_type=msg_type,
        summary=summary,
        occur_time=occur_time,
        received_at_utc=received_at,
        raw_json=raw_json,
    )

    # ---- (C) Best-effort flush with throttle ----
    maybe_flush_sheets()


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
    """
    Join devices to show device_name in Recent events.
    """
    conn = db_connect()
    rows = conn.execute(
        """
        SELECT
            e.device_id,
            COALESCE(d.device_name, '') AS device_name,
            e.msg_type,
            e.summary,
            e.occur_time,
            e.received_at_utc
        FROM events e
        LEFT JOIN devices d ON d.device_id = e.device_id
        ORDER BY e.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_callback_inbox(headers: dict, body_text: str):
    """
    Store raw callback payload only when DEBUG_CALLBACK_INBOX=1
    """
    if not DEBUG_CALLBACK_INBOX:
        return
    body_text = (body_text or "")[:CALLBACK_INBOX_MAX_BODY_CHARS]
    conn = db_connect()
    conn.execute(
        "INSERT INTO callback_inbox(received_at_utc, headers_json, body_text) VALUES(?,?,?)",
        (now_kyiv_iso(), json.dumps(headers, ensure_ascii=False), body_text),
    )
    conn.execute(
        f"DELETE FROM callback_inbox WHERE id NOT IN (SELECT id FROM callback_inbox ORDER BY id DESC LIMIT {int(CALLBACK_INBOX_MAX)})"
    )
    conn.commit()
    conn.close()


# -----------------------------
# Google Drive / Sheets helpers
# -----------------------------
DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
_drive_service = None
_sheets_service = None

_last_flush_ts = 0.0


def google_enabled() -> bool:
    return GDRIVE_EVENTS_ENABLED and bool(GDRIVE_SA_JSON_B64)


def get_drive_service():
    global _drive_service
    if _drive_service is not None:
        return _drive_service
    if not GDRIVE_SA_JSON_B64:
        raise RuntimeError("Missing GDRIVE_SA_JSON_B64")
    sa_info = json.loads(base64.b64decode(GDRIVE_SA_JSON_B64).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=DRIVE_SCOPES)
    _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_service


def get_sheets_service():
    global _sheets_service
    if _sheets_service is not None:
        return _sheets_service
    if not GDRIVE_SA_JSON_B64:
        raise RuntimeError("Missing GDRIVE_SA_JSON_B64")
    sa_info = json.loads(base64.b64decode(GDRIVE_SA_JSON_B64).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=DRIVE_SCOPES)
    _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _sheets_service


def drive_find_file_id(service, folder_id: str, name: str, mime_type: str | None = None):
    q = f"'{folder_id}' in parents and name='{name}' and trashed=false"
    if mime_type:
        q += f" and mimeType='{mime_type}'"
    res = service.files().list(q=q, fields="files(id,name,mimeType)").execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def drive_ensure_folder(service, parent_id: str, folder_name: str) -> str:
    q = (
        f"'{parent_id}' in parents and trashed=false and "
        f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
    )
    res = service.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    created = service.files().create(
        body={
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id",
    ).execute()
    return created["id"]


def resolve_imou_project_folder_id() -> str:
    """
    Priority:
    1) GDRIVE_IMOU_PROJECT_FOLDER_ID
    2) ensure folder 'imou_project' inside GDRIVE_ROOT_FOLDER_ID
    """
    if GDRIVE_IMOU_PROJECT_FOLDER_ID:
        return GDRIVE_IMOU_PROJECT_FOLDER_ID

    if not GDRIVE_ROOT_FOLDER_ID:
        raise RuntimeError(
            "Set GDRIVE_IMOU_PROJECT_FOLDER_ID (recommended) or GDRIVE_ROOT_FOLDER_ID (to create/find 'imou_project')."
        )

    drive = get_drive_service()
    return drive_ensure_folder(drive, GDRIVE_ROOT_FOLDER_ID, "imou_project")


def drive_create_spreadsheet(service, folder_id: str, name: str) -> str:
    created = service.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        },
        fields="id",
    ).execute()
    return created["id"]


def ensure_events_spreadsheet_id() -> str:
    """
    Returns spreadsheet id:
    - env GDRIVE_EVENTS_SPREADSHEET_ID, else:
    - find by name in imou_project folder, else create
    Caches into kv.
    """
    if GDRIVE_EVENTS_SPREADSHEET_ID:
        return GDRIVE_EVENTS_SPREADSHEET_ID

    cached = kv_get("gsheet_events_spreadsheet_id")
    if cached:
        return cached

    drive = get_drive_service()
    folder_id = resolve_imou_project_folder_id()

    sid = drive_find_file_id(
        drive,
        folder_id,
        GDRIVE_EVENTS_SPREADSHEET_NAME,
        mime_type="application/vnd.google-apps.spreadsheet",
    )
    if not sid:
        sid = drive_create_spreadsheet(drive, folder_id, GDRIVE_EVENTS_SPREADSHEET_NAME)

    kv_set("gsheet_events_spreadsheet_id", sid)
    return sid


def ensure_tab_and_header():
    """    Ensures sheet tab exists.

    IMPORTANT for backward compatibility:
    - If the sheet already has a header row (any non-empty cell in A1:Z1), we DO NOT overwrite it.
    - We only write our default header when the first row is empty.

    This lets you keep your existing Google Sheet (old columns, formulas, filters) and continue appending rows.
    """
    sid = ensure_events_spreadsheet_id()
    sheets = get_sheets_service()

    header_done = kv_get("gsheet_events_header_done")
    if header_done == "1":
        return

    # Ensure tab exists
    meta = sheets.spreadsheets().get(spreadsheetId=sid).execute()
    tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if GDRIVE_EVENTS_TAB_NAME not in tabs:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests": [{"addSheet": {"properties": {"title": GDRIVE_EVENTS_TAB_NAME}}}]},
        ).execute()

    # Check if header already exists (do not overwrite)
    try:
        r = sheets.spreadsheets().values().get(
            spreadsheetId=sid,
            range=f"{GDRIVE_EVENTS_TAB_NAME}!A1:Z1",
        ).execute()
        existing = (r.get("values") or [[]])[0] if r.get("values") else []
    except Exception:
        existing = []

    has_header = any(str(c).strip() for c in (existing or []))
    if not has_header:
        header = [
            "received_at_kyiv",
            "occur_time",
            "device_id",
            "device_name",
            "msg_type",
            "summary",
            "raw_json",
        ]
        sheets.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{GDRIVE_EVENTS_TAB_NAME}!A1:G1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

    kv_set("gsheet_events_header_done", "1")


def enqueue_event_for_sheets(
    device_id: str,
    device_name: str,
    msg_type: str,
    summary: str,
    occur_time: str,
    received_at_utc: str,
    raw_json: str,
):
    """
    Insert-or-ignore into sheet_queue. This is the durable "ALL events" store (independent from events retention).
    """
    uid_src = f"{received_at_utc}|{occur_time}|{device_id}|{msg_type}|{summary}"
    uid = sha256_hex(uid_src)

    row = {
        "received_at_utc": received_at_utc,
        "occur_time": occur_time,
        "device_id": device_id,
        "device_name": device_name or "",
        "msg_type": msg_type,
        "summary": summary,
        "raw_json": raw_json,
    }

    conn = db_connect()
    conn.execute(
        """
        INSERT OR IGNORE INTO sheet_queue(uid,row_json,created_at_utc,sent,sent_at_utc)
        VALUES(?,?,?,0,NULL)
        """,
        (uid, json.dumps(row, ensure_ascii=False), now_kyiv_iso()),
    )
    conn.commit()
    conn.close()


def sheets_queue_stats() -> dict:
    conn = db_connect()
    total = conn.execute("SELECT COUNT(1) AS c FROM sheet_queue").fetchone()["c"]
    unsent = conn.execute("SELECT COUNT(1) AS c FROM sheet_queue WHERE sent=0").fetchone()["c"]
    conn.close()
    return {"total": int(total), "unsent": int(unsent)}


def flush_sheets(max_rows: int | None = None) -> dict:
    """
    Flush unsent rows to Google Sheets.
    """
    if not google_enabled():
        return {"ok": False, "reason": "google disabled or missing GDRIVE_SA_JSON_B64"}

    try:
        ensure_tab_and_header()
        sid = ensure_events_spreadsheet_id()
        sheets = get_sheets_service()

        limit = max_rows or GDRIVE_EVENTS_APPEND_BATCH

        conn = db_connect()
        rows = conn.execute(
            "SELECT uid, row_json FROM sheet_queue WHERE sent=0 ORDER BY created_at_utc ASC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()

        if not rows:
            return {"ok": True, "flushed": 0}

        values = []
        uids = []
        for r in rows:
            uids.append(r["uid"])
            obj = json.loads(r["row_json"])
            values.append(
                [
                    obj.get("received_at_utc", ""),
                    obj.get("occur_time", ""),
                    obj.get("device_id", ""),
                    obj.get("device_name", ""),
                    obj.get("msg_type", ""),
                    obj.get("summary", ""),
                    obj.get("raw_json", ""),
                ]
            )

        # Append after header (A2)
        sheets.spreadsheets().values().append(
            spreadsheetId=sid,
            range=f"{GDRIVE_EVENTS_TAB_NAME}!A2",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()

        # Mark sent
        conn = db_connect()
        now_sent = now_kyiv_iso()
        conn.executemany(
            "UPDATE sheet_queue SET sent=1, sent_at_utc=? WHERE uid=?",
            [(now_sent, uid) for uid in uids],
        )
        conn.commit()
        conn.close()

        return {"ok": True, "flushed": len(uids)}
    except Exception as e:
        # Don't fail main flow
        return {"ok": False, "error": str(e)}


def maybe_flush_sheets():
    global _last_flush_ts
    if not google_enabled():
        return

    # throttle
    now_ts = time.time()
    if now_ts - _last_flush_ts < max(1, GDRIVE_FLUSH_INTERVAL_SEC):
        return

    st = sheets_queue_stats()
    # flush if we have any unsent
    if st["unsent"] <= 0:
        _last_flush_ts = now_ts
        return

    res = flush_sheets(GDRIVE_EVENTS_APPEND_BATCH)
    _last_flush_ts = now_ts
    if not res.get("ok"):
        # keep quiet, but log to app logger
        app.logger.warning(f"Google Sheets flush failed: {res}")


# -----------------------------
# Imou Open Platform client
# -----------------------------
def imou_base_url() -> str:
    if not IMOU_DATACENTER:
        raise RuntimeError("IMOU_DATACENTER is not set")
    return f"https://openapi-{IMOU_DATACENTER}.easy4ip.com/openapi"


def imou_sign(app_secret: str, ts: int, nonce: str) -> str:
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

    data = imou_post("accessToken", {})
    token = data["accessToken"]
    expire_sec = int(data.get("expireTime", 0))
    expires_at = int(time.time()) + max(0, expire_sec) - 600
    kv_set("imou_access_token_json", json.dumps({"token": token, "expires_at": expires_at}))
    return token


def imou_set_message_callback(callback_url: str, status: str = "on"):
    token = imou_get_admin_token()
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
    return imou_post("deviceOnline", {"token": token, "deviceId": device_id})


def imou_list_device_details_by_ids(device_ids: list[str]) -> list[dict]:
    token = imou_get_admin_token()
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
            "gsheets": {
                "enabled": google_enabled(),
                "queue": sheets_queue_stats(),
                "spreadsheet_id": (GDRIVE_EVENTS_SPREADSHEET_ID or kv_get("gsheet_events_spreadsheet_id") or ""),
                "tab": GDRIVE_EVENTS_TAB_NAME,
            },
        }
    )


def callback_endpoint() -> str:
    base = PUBLIC_BASE_URL
    if not base:
        try:
            base = request.url_root.rstrip("/")
        except Exception:
            base = ""
    return f"{base}/imou/callback" if base else "/imou/callback"


@app.get("/admin/get-callback")
def admin_get_callback():
    require_admin()
    return jsonify(imou_get_message_callback())


@app.get("/admin/last-callbacks")
def admin_last_callbacks():
    require_admin()
    conn = db_connect()
    rows = conn.execute(
        "SELECT id, received_at_utc, body_text FROM callback_inbox ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.post("/admin/clear-events")
def admin_clear_events():
    require_admin()
    conn = db_connect()
    conn.execute("DELETE FROM events")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "cleared": "events"})


@app.post("/admin/clear-callback-inbox")
def admin_clear_callback_inbox():
    require_admin()
    conn = db_connect()
    conn.execute("DELETE FROM callback_inbox")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "cleared": "callback_inbox"})


@app.get("/admin/gsheets-status")
def admin_gsheets_status():
    require_admin()
    return jsonify(
        {
            "enabled": google_enabled(),
            "queue": sheets_queue_stats(),
            "spreadsheet_id": (GDRIVE_EVENTS_SPREADSHEET_ID or kv_get("gsheet_events_spreadsheet_id") or ""),
            "tab": GDRIVE_EVENTS_TAB_NAME,
            "folder_id": (GDRIVE_IMOU_PROJECT_FOLDER_ID or ""),
        }
    )


@app.post("/admin/flush-sheets")
def admin_flush_sheets():
    require_admin()
    body = request.get_json(silent=True) or {}
    n = int(body.get("max_rows", 0) or 0)
    res = flush_sheets(n if n > 0 else None)
    return jsonify(res)


@app.post("/admin/test-telegram")
def admin_test_telegram():
    require_admin()
    body = request.get_json(silent=True) or {}
    text = (body.get("text") or f"{TELEGRAM_PARKING_DEVICE_NAME} ({TELEGRAM_PARKING_DEVICE_ID}) - deviceStatus: online").strip()
    res = telegram_send_message(text)
    return jsonify(res)


@app.get("/admin/dtek-forecast")
def admin_dtek_forecast():
    """Debug endpoint: fetch & parse DTEK forecast (does NOT send anything)."""
    require_admin()
    force = request.args.get("force", "0").strip() in ("1", "true", "yes", "y", "on")
    res = dtek_get_forecast_cached(force=force)
    return jsonify(res)


@app.post("/admin/dtek-notify")
def admin_dtek_notify():
    """Debug endpoint: send current DTEK forecast to the configured Telegram channel."""
    require_admin()
    res = dtek_get_forecast_cached(force=True)
    if not telegram_enabled():
        return jsonify({"ok": False, "error": "telegram disabled or missing TELEGRAM_* env"}), 400

    msg = "⏱️ Прогноз ДТЕК: "
    if res.get("restore_at_kyiv"):
        msg += f"очікуване включення {res.get('restore_at_kyiv')}"
    elif res.get("note"):
        msg += res.get("note")
    else:
        msg += "дані недоступні"

    send_res = telegram_send_message(msg)
    return jsonify({"forecast": res, "telegram": send_res})




@app.post("/admin/clear-sheets-queue")
def admin_clear_sheets_queue():
    require_admin()
    conn = db_connect()
    conn.execute("DELETE FROM sheet_queue")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "cleared": "sheet_queue"})


@app.get("/")
def index():
    devices = get_devices()
    events = get_recent_events(30)
    gs = {"enabled": google_enabled(), "queue": sheets_queue_stats()}
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
  <div class="muted" style="margin-top:-8px; margin-bottom:12px;">
    <a href="/charts">Charts</a>
  </div>

  <div class="row">
    <div class="card">
      <div><b>Message Callback Address</b></div>
      <div class="muted">Set this URL in Imou (or call <code>/admin/set-callback</code>):</div>
      <div style="margin-top:8px;"><code id="cb">{{ cb }}</code></div>
      <div class="muted" style="margin-top:8px;">Callback must return HTTP 200.</div>
    </div>

    <div class="card">
      <div><b>Google Sheets events</b></div>
      <div class="muted">Enabled: <b>{{ "yes" if gs.enabled else "no" }}</b></div>
      <div class="muted">Queue (unsent/total): <b>{{ gs.queue.unsent }}</b> / <b>{{ gs.queue.total }}</b></div>
      <div class="muted" style="margin-top:8px;">Stores ALL events (independent from SQLite retention).</div>
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
      <div style="margin-top:10px;">
        <div class="muted">Google Sheets:</div>
        <button onclick="adminPost('/admin/flush-sheets')">Flush sheets</button>
        <button onclick="adminPost('/admin/clear-sheets-queue')">Clear sheets queue</button>
      </div>
      <div style="margin-top:10px;">
        <div class="muted">Maintenance:</div>
        <button onclick="adminPost('/admin/clear-events')">Clear events</button>
        <button onclick="adminPost('/admin/clear-callback-inbox')">Clear callback inbox</button>
      </div>
      <div class="muted" style="margin-top:10px;">
        Tip: pass admin key header <code>X-Admin-Key</code>.
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
        <th>Last seen (Kyiv)</th>
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
        <th>Received (Kyiv)</th>
      </tr>
    </thead>
    <tbody id="eventrows">
      {% for e in events %}
      <tr>
        <td>
          {% if e.device_name %}
            {{ e.device_name }}
            <div class="muted"><code>{{ e.device_id }}</code></div>
          {% else %}
            <code>{{ e.device_id }}</code>
          {% endif %}
        </td>
        <td>{{ e.msg_type }}</td>
        <td class="muted">{{ e.summary }}</td>
        <td class="muted">{{ e.occur_time }}</td>
        <td class="muted">{{ e.received_at_utc }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>

<script>
  const ADMIN_KEY_PRESENT = "{{ admin_key_present }}";

  async function adminPost(path, body) {
    if (!ADMIN_KEY_PRESENT) {
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

  function esc(s) {
    return (s ?? "").toString()
      .replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")
      .replaceAll('"',"&quot;").replaceAll("'","&#039;");
  }

  // Auto-refresh every 10s: Devices + Recent events
  setInterval(async () => {
    try {
      const r = await fetch("/api/status");
      if (!r.ok) return;
      const data = await r.json();

      // Devices
      const tbody = document.getElementById("devrows");
      tbody.innerHTML = "";
      (data.devices || []).forEach(d => {
        const st = d.status || "unknown";
        const ok = (st === "online" || st === "1");
        const bad = (st === "offline" || st === "0");
        const cls = ok ? "ok" : (bad ? "bad" : "");
        tbody.innerHTML += `
          <tr>
            <td>${esc(d.device_name || "")}</td>
            <td><code>${esc(d.device_id)}</code></td>
            <td><span class="pill ${cls}">${esc(st)}</span></td>
            <td class="muted">${esc(d.last_seen_utc || "")}</td>
            <td class="muted">${esc(d.last_event_summary || "")}</td>
          </tr>`;
      });

      // Callback URL
      document.getElementById("cb").textContent = data.callback_endpoint || "";

      // Recent events
      const et = document.getElementById("eventrows");
      et.innerHTML = "";
      (data.recent_events || []).slice(0, 30).forEach(e => {
        const nameCell = e.device_name
          ? `${esc(e.device_name)}<div class="muted"><code>${esc(e.device_id)}</code></div>`
          : `<code>${esc(e.device_id)}</code>`;
        et.innerHTML += `
          <tr>
            <td>${nameCell}</td>
            <td>${esc(e.msg_type || "")}</td>
            <td class="muted">${esc(e.summary || "")}</td>
            <td class="muted">${esc(e.occur_time || "")}</td>
            <td class="muted">${esc(e.received_at_utc || "")}</td>
          </tr>`;
      });
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
        gs=gs,
    )


@app.post("/imou/callback")
def imou_callback():
    raw_text = request.get_data(as_text=True) or ""
    save_callback_inbox(dict(request.headers), raw_text)

    try:
        raw = request.get_json(silent=True)
        if raw is None:
            raw = json.loads(raw_text or "{}")

        if isinstance(raw, dict) and isinstance(raw.get("params"), dict):
            raw = raw["params"]

        messages = raw if isinstance(raw, list) else [raw]

        for msg in messages:
            if not isinstance(msg, dict):
                add_event("__unknown__", "raw", "non-dict payload", "", {"raw": raw_text})
                continue

            device_id = (
                (msg.get("deviceId") or "").strip()
                or (msg.get("did") or "").strip()
                or "__unknown__"
            )

            # If callback sometimes includes deviceName, store it immediately
            device_name = (msg.get("deviceName") or msg.get("device_name") or "").strip()
            if device_name and device_id and device_id != "__unknown__":
                upsert_device(device_id, device_name=device_name)

            msg_type = (msg.get("msgType") or msg.get("type") or "unknown").strip()
            occur_time = str(msg.get("occurTime") or msg.get("time") or "")

            now_iso = now_kyiv_iso()

            status = ""
            interval_note = ""
            prev_status = ""
            prev_changed_at = ""

            if msg_type in ("online", "offline"):
                status = msg_type
                summary = f"deviceStatus: {status}"
            elif msg_type == "deviceStatus":
                status = _normalize_status(msg.get("status") or "")
                summary = f"deviceStatus: {status or 'unknown'}"
            elif msg_type == "alarm":
                summary = f"alarm: {(msg.get('alarmName') or '')} {(msg.get('alarmType') or '')}".strip()
            else:
                summary = msg_type

            # Build power interval note ONLY for real online/offline transitions.
            if status in ("online", "offline") and device_id and device_id != "__unknown__":
                prev_status, prev_changed_at = get_device_status_info(device_id)
                if prev_status in ("online", "offline") and prev_changed_at and prev_status != status:
                    interval_note = _build_power_interval_note(status, prev_changed_at, now_iso)
                    if interval_note:
                        # Keep Google Sheets schema unchanged: duration is embedded into the same 'summary' cell.
                        summary = f"{summary} ({interval_note})"

            add_event(device_id, msg_type, summary, occur_time, msg)

            # Telegram notify on status changes (for parking device only)
            if status in ("online", "offline") and prev_status != status:
                maybe_notify_telegram_device_status(device_id, status, interval_note)

            fields = {"last_seen_utc": now_iso, "last_event_summary": summary}
            if status:
                fields["status"] = status

            # Track when the current ONLINE/OFFLINE status started (used for power on/off interval calculation)
            if status in ("online", "offline"):
                if not prev_changed_at:
                    # baseline if missing
                    fields["status_changed_at_kyiv"] = now_iso
                elif prev_status != status:
                    fields["status_changed_at_kyiv"] = now_iso

            upsert_device(device_id, **fields)



    except Exception:
        app.logger.exception("IMOU CALLBACK processing error")

    return "OK", 200




# -----------------------------
# Charts (web dashboard)
# -----------------------------
_CHARTS_CACHE = {}
CHARTS_CACHE_TTL_SEC = int(os.getenv("CHARTS_CACHE_TTL_SEC", "300"))
CHARTS_SMOOTH_SIGMA = float(os.getenv("CHARTS_SMOOTH_SIGMA", "2.0"))
CHARTS_MIN_OUTAGE_MINUTES = int(os.getenv("CHARTS_MIN_OUTAGE_MINUTES", "10"))  # ignore outages shorter than this on charts


def _charts_cache_get(key: str):
    hit = _CHARTS_CACHE.get(key)
    if not hit:
        return None
    if time.time() - hit.get("ts", 0) > CHARTS_CACHE_TTL_SEC:
        return None
    return hit.get("val")


def _charts_cache_set(key: str, val):
    _CHARTS_CACHE[key] = {"ts": time.time(), "val": val}


def _gaussian_smooth_60(counts, sigma: float = 2.0):
    """Gaussian smoothing for 60 bins. Returns list[float] length 60."""
    counts = list(counts or [])
    if len(counts) != 60:
        counts = (counts + [0]*60)[:60]

    sigma = float(sigma or 0.0)
    if sigma <= 0:
        return [float(x) for x in counts]

    radius = int(max(3, math.ceil(sigma * 3)))
    kernel = [math.exp(-(x*x) / (2*sigma*sigma)) for x in range(-radius, radius + 1)]
    s = sum(kernel) or 1.0
    kernel = [k / s for k in kernel]

    out = [0.0] * 60
    for i in range(60):
        v = 0.0
        for j, k in enumerate(kernel):
            idx = i + (j - radius)
            if 0 <= idx < 60:
                v += counts[idx] * k
        out[i] = v
    return out


def _safe_fromiso_minute(ts: str):
    """Parse ISO timestamp and return minute (0..59) or None."""
    dt = _parse_iso_dt(ts)
    if not dt:
        return None
    try:
        return int(dt.minute)
    except Exception:
        return None




def _dt_ensure_tz(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware. If naive, assume Kyiv."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KYIV_TZ)
    return dt


def _extract_status_events(values, device_id: str, i_time: int, i_dev: int, i_type: int):
    """Return sorted list of (dt, status) for a given device from sheet rows."""
    out = []
    did = (device_id or '').strip()
    for row in values[1:]:
        if not row or len(row) <= max(i_time, i_dev, i_type):
            continue
        if (row[i_dev] or '').strip() != did:
            continue
        st = (row[i_type] or '').strip()
        if st not in ('online','offline'):
            continue
        dt = _parse_iso_dt(row[i_time])
        if not dt:
            continue
        dt = _dt_ensure_tz(dt)
        out.append((dt, st))
    out.sort(key=lambda x: x[0])

    # Collapse consecutive duplicates (online-online, offline-offline)
    collapsed = []
    last_st = None
    for dt, st in out:
        if st == last_st:
            continue
        collapsed.append((dt, st))
        last_st = st
    return collapsed


def _compute_outages_from_status_events(events):
    """From status-change events (dt, status), compute list of outages as (start_dt, end_dt or None)."""
    outages = []
    cur_off_start = None

    for dt, st in events:
        if st == 'offline':
            # start of outage
            cur_off_start = dt
        elif st == 'online':
            if cur_off_start is not None:
                outages.append((cur_off_start, dt))
                cur_off_start = None

    # ongoing outage
    if cur_off_start is not None:
        outages.append((cur_off_start, None))

    return outages


def compute_minute_hist_from_gsheets(device_id: str, msg_type: str):
    """Reads Events sheet and computes minute histogram for device_id + msg_type.

    IMPORTANT: For power charts we ignore short outages (< CHARTS_MIN_OUTAGE_MINUTES).
    - If msg_type == 'offline': histogram of outage START minutes, for outages >= threshold.
    - If msg_type == 'online' : histogram of outage END minutes (power restored), for outages >= threshold.

    This logic affects ONLY charts; Telegram notifications & DB logic are unchanged.
    """
    if not google_enabled():
        return {
            'ok': False,
            'error': 'Google is disabled (missing GDRIVE_SA_JSON_B64)',
            'device_id': device_id,
            'msg_type': msg_type,
            'minutes': list(range(60)),
            'counts': [0] * 60,
            'smoothed': [0] * 60,
            'total': 0,
            'source': 'gsheets',
            'min_outage_minutes': CHARTS_MIN_OUTAGE_MINUTES,
        }

    try:
        sid = ensure_events_spreadsheet_id()
        sheets = get_sheets_service()

        # Efficient range: enough for time/device/msg_type for the standard header
        rng = f"{GDRIVE_EVENTS_TAB_NAME}!A:E"
        values = sheets.spreadsheets().values().get(spreadsheetId=sid, range=rng).execute().get('values', [])

        if not values or len(values) < 2:
            return {
                'ok': True,
                'device_id': device_id,
                'msg_type': msg_type,
                'minutes': list(range(60)),
                'counts': [0] * 60,
                'smoothed': [0] * 60,
                'total': 0,
                'source': 'gsheets',
                'min_outage_minutes': CHARTS_MIN_OUTAGE_MINUTES,
            }

        header = values[0]

        # Default indices for our standard header/order:
        # A received_at_kyiv, B occur_time, C device_id, D device_name, E msg_type
        i_time, i_dev, i_type = 0, 2, 4

        # Map by header names if present
        try:
            if isinstance(header, list) and header:
                if 'received_at_kyiv' in header:
                    i_time = header.index('received_at_kyiv')
                elif 'received_at_utc' in header:
                    i_time = header.index('received_at_utc')
                if 'device_id' in header:
                    i_dev = header.index('device_id')
                if 'msg_type' in header:
                    i_type = header.index('msg_type')
        except Exception:
            pass

        # Build status-change timeline and outages
        events = _extract_status_events(values, device_id=device_id, i_time=i_time, i_dev=i_dev, i_type=i_type)
        outages = _compute_outages_from_status_events(events)

        threshold_sec = int(CHARTS_MIN_OUTAGE_MINUTES) * 60
        now_dt = datetime.now(KYIV_TZ)

        counts = [0] * 60
        total = 0

        for start_dt, end_dt in outages:
            end_eff = end_dt or now_dt
            # Ensure tz compatible
            start_dt = _dt_ensure_tz(start_dt)
            end_eff = _dt_ensure_tz(end_eff)

            dur = int((end_eff - start_dt).total_seconds())
            if dur < threshold_sec:
                continue

            if msg_type == 'offline':
                minute = int(start_dt.minute)
            else:  # online
                if end_dt is None:
                    continue  # no restore yet
                minute = int(end_dt.minute)

            if 0 <= minute <= 59:
                counts[minute] += 1
                total += 1

        smoothed = _gaussian_smooth_60(counts, sigma=CHARTS_SMOOTH_SIGMA)

        return {
            'ok': True,
            'device_id': device_id,
            'msg_type': msg_type,
            'minutes': list(range(60)),
            'counts': counts,
            'smoothed': smoothed,
            'total': total,
            'source': 'gsheets',
            'min_outage_minutes': CHARTS_MIN_OUTAGE_MINUTES,
        }

    except Exception as e:
        return {
            'ok': False,
            'error': str(e),
            'device_id': device_id,
            'msg_type': msg_type,
            'minutes': list(range(60)),
            'counts': [0] * 60,
            'smoothed': [0] * 60,
            'total': 0,
            'source': 'gsheets',
            'min_outage_minutes': CHARTS_MIN_OUTAGE_MINUTES,
        }



@app.get("/api/charts/minute-hist")
def api_charts_minute_hist():
    device_id = (request.args.get("device_id") or (TELEGRAM_PARKING_DEVICE_ID or "")).strip()
    msg_type = (request.args.get("msg_type") or "offline").strip()

    # Basic safety
    if msg_type not in ("offline", "online"):
        msg_type = "offline"

    cache_key = f"minute_hist:{device_id}:{msg_type}"
    cached = _charts_cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    data = compute_minute_hist_from_gsheets(device_id, msg_type)
    _charts_cache_set(cache_key, data)
    return jsonify(data)





def compute_power_ratio_parking_from_gsheets():
    """Compute total ONLINE vs OFFLINE hours for the Parking device, based on status changes in Events sheet."""
    device_id = (TELEGRAM_PARKING_DEVICE_ID or '').strip()
    device_name = (TELEGRAM_PARKING_DEVICE_NAME or 'Парковка').strip()

    if not google_enabled():
        return {
            'ok': False,
            'error': 'Google is disabled (missing GDRIVE_SA_JSON_B64)',
            'device_id': device_id,
            'device_name': device_name,
            'online_hours': 0.0,
            'offline_hours': 0.0,
            'total_hours': 0.0,
            'range_from': None,
            'range_to': None,
            'source': 'gsheets',
        }

    try:
        sid = ensure_events_spreadsheet_id()
        sheets = get_sheets_service()

        rng = f"{GDRIVE_EVENTS_TAB_NAME}!A:E"
        values = sheets.spreadsheets().values().get(spreadsheetId=sid, range=rng).execute().get('values', [])
        if not values or len(values) < 2:
            return {
                'ok': True,
                'device_id': device_id,
                'device_name': device_name,
                'online_hours': 0.0,
                'offline_hours': 0.0,
                'total_hours': 0.0,
                'range_from': None,
                'range_to': None,
                'source': 'gsheets',
            }

        header = values[0]
        i_time, i_dev, i_type = 0, 2, 4
        try:
            if isinstance(header, list) and header:
                if 'received_at_kyiv' in header:
                    i_time = header.index('received_at_kyiv')
                elif 'received_at_utc' in header:
                    i_time = header.index('received_at_utc')
                if 'device_id' in header:
                    i_dev = header.index('device_id')
                if 'msg_type' in header:
                    i_type = header.index('msg_type')
        except Exception:
            pass

        events = _extract_status_events(values, device_id=device_id, i_time=i_time, i_dev=i_dev, i_type=i_type)
        if not events:
            return {
                'ok': True,
                'device_id': device_id,
                'device_name': device_name,
                'online_hours': 0.0,
                'offline_hours': 0.0,
                'total_hours': 0.0,
                'range_from': None,
                'range_to': None,
                'source': 'gsheets',
            }

        online_sec = 0
        offline_sec = 0

        last_dt, last_st = events[0]
        range_from = last_dt

        for dt, st in events[1:]:
            seg = int((dt - last_dt).total_seconds())
            if seg > 0:
                if last_st == 'online':
                    online_sec += seg
                elif last_st == 'offline':
                    offline_sec += seg
            last_dt, last_st = dt, st

        # extend the last segment to now (assume status persisted)
        now_dt = datetime.now(KYIV_TZ)
        seg = int((now_dt - last_dt).total_seconds())
        if seg > 0:
            if last_st == 'online':
                online_sec += seg
            elif last_st == 'offline':
                offline_sec += seg

        total_sec = online_sec + offline_sec

        return {
            'ok': True,
            'device_id': device_id,
            'device_name': device_name,
            'online_hours': round(online_sec / 3600.0, 2),
            'offline_hours': round(offline_sec / 3600.0, 2),
            'total_hours': round(total_sec / 3600.0, 2),
            'range_from': range_from.isoformat() if range_from else None,
            'range_to': now_dt.isoformat(),
            'source': 'gsheets',
        }

    except Exception as e:
        return {
            'ok': False,
            'error': str(e),
            'device_id': device_id,
            'device_name': device_name,
            'online_hours': 0.0,
            'offline_hours': 0.0,
            'total_hours': 0.0,
            'range_from': None,
            'range_to': None,
            'source': 'gsheets',
        }


@app.get('/api/charts/power-ratio')
def api_charts_power_ratio():
    cache_key = f"power_ratio:{TELEGRAM_PARKING_DEVICE_ID or ''}"
    cached = _charts_cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    data = compute_power_ratio_parking_from_gsheets()
    _charts_cache_set(cache_key, data)
    return jsonify(data)


# -----------------------------
# Charts: Daily power ratio (Parking)
# -----------------------------
def _accumulate_daily(bucket: dict, start_dt: datetime, end_dt: datetime, st: str):
    """Accumulate seconds for status st between [start_dt, end_dt) into bucket by Kyiv date."""
    if not start_dt or not end_dt:
        return
    start_dt = _dt_ensure_tz(start_dt).astimezone(KYIV_TZ)
    end_dt = _dt_ensure_tz(end_dt).astimezone(KYIV_TZ)
    if end_dt <= start_dt:
        return

    cur = start_dt
    # split by midnight boundaries in Kyiv
    while cur.date() < end_dt.date():
        nxt_midnight = datetime(cur.year, cur.month, cur.day, tzinfo=KYIV_TZ) + timedelta(days=1)
        sec = int((nxt_midnight - cur).total_seconds())
        if sec > 0:
            key = cur.date().isoformat()
            b = bucket.setdefault(key, {'online_sec': 0, 'offline_sec': 0})
            if st == 'online':
                b['online_sec'] += sec
            elif st == 'offline':
                b['offline_sec'] += sec
        cur = nxt_midnight

    sec = int((end_dt - cur).total_seconds())
    if sec > 0:
        key = cur.date().isoformat()
        b = bucket.setdefault(key, {'online_sec': 0, 'offline_sec': 0})
        if st == 'online':
            b['online_sec'] += sec
        elif st == 'offline':
            b['offline_sec'] += sec


def compute_power_ratio_daily_parking_from_gsheets():
    """Daily ONLINE vs OFFLINE hours by date for the Parking device (Kyiv dates), based on Events sheet."""
    device_id = (TELEGRAM_PARKING_DEVICE_ID or '').strip()
    device_name = (TELEGRAM_PARKING_DEVICE_NAME or 'Парковка').strip()

    if not google_enabled():
        return {
            'ok': False,
            'error': 'Google is disabled (missing GDRIVE_SA_JSON_B64)',
            'device_id': device_id,
            'device_name': device_name,
            'dates': [],
            'online_hours': [],
            'offline_hours': [],
            'offline_pct': [],
            'range_from': None,
            'range_to': None,
            'source': 'gsheets',
        }

    try:
        sid = ensure_events_spreadsheet_id()
        sheets = get_sheets_service()

        rng = f"{GDRIVE_EVENTS_TAB_NAME}!A:E"
        values = sheets.spreadsheets().values().get(spreadsheetId=sid, range=rng).execute().get('values', [])
        if not values or len(values) < 2:
            return {
                'ok': True,
                'device_id': device_id,
                'device_name': device_name,
                'dates': [],
                'online_hours': [],
                'offline_hours': [],
                'offline_pct': [],
                'range_from': None,
                'range_to': None,
                'source': 'gsheets',
            }

        header = values[0]
        i_time, i_dev, i_type = 0, 2, 4
        try:
            if isinstance(header, list) and header:
                if 'received_at_kyiv' in header:
                    i_time = header.index('received_at_kyiv')
                elif 'received_at_utc' in header:
                    i_time = header.index('received_at_utc')
                if 'device_id' in header:
                    i_dev = header.index('device_id')
                if 'msg_type' in header:
                    i_type = header.index('msg_type')
        except Exception:
            pass

        events = _extract_status_events(values, device_id=device_id, i_time=i_time, i_dev=i_dev, i_type=i_type)
        if not events:
            return {
                'ok': True,
                'device_id': device_id,
                'device_name': device_name,
                'dates': [],
                'online_hours': [],
                'offline_hours': [],
                'offline_pct': [],
                'range_from': None,
                'range_to': None,
                'source': 'gsheets',
            }

        bucket = {}
        last_dt, last_st = events[0]
        last_dt = _dt_ensure_tz(last_dt).astimezone(KYIV_TZ)
        range_from = last_dt

        for dt, st in events[1:]:
            dt = _dt_ensure_tz(dt).astimezone(KYIV_TZ)
            _accumulate_daily(bucket, last_dt, dt, last_st)
            last_dt, last_st = dt, st

        now_dt = datetime.now(KYIV_TZ)
        _accumulate_daily(bucket, last_dt, now_dt, last_st)

        dates = sorted(bucket.keys())
        online_hours = []
        offline_hours = []
        offline_pct = []
        for d in dates:
            on = bucket[d]['online_sec']
            off = bucket[d]['offline_sec']
            total = on + off
            online_hours.append(round(on / 3600.0, 2))
            offline_hours.append(round(off / 3600.0, 2))
            pct = (off / total * 100.0) if total > 0 else 0.0
            offline_pct.append(round(pct, 2))

        return {
            'ok': True,
            'device_id': device_id,
            'device_name': device_name,
            'dates': dates,
            'online_hours': online_hours,
            'offline_hours': offline_hours,
            'offline_pct': offline_pct,
            'range_from': range_from.isoformat() if range_from else None,
            'range_to': now_dt.isoformat(),
            'source': 'gsheets',
        }

    except Exception as e:
        return {
            'ok': False,
            'error': str(e),
            'device_id': device_id,
            'device_name': device_name,
            'dates': [],
            'online_hours': [],
            'offline_hours': [],
            'offline_pct': [],
            'range_from': None,
            'range_to': None,
            'source': 'gsheets',
        }


@app.get('/api/charts/power-daily')
def api_charts_power_daily():
    cache_key = f"power_daily:{TELEGRAM_PARKING_DEVICE_ID or ''}"
    cached = _charts_cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    data = compute_power_ratio_daily_parking_from_gsheets()
    _charts_cache_set(cache_key, data)
    return jsonify(data)


@app.get("/charts")
def charts_page():
    devices = get_devices()
    default_device = (TELEGRAM_PARKING_DEVICE_ID or (devices[0]["device_id"] if devices else "")).strip()

    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Charts</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 20px; }
    .topbar { display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 14px; }
    .row { display:flex; gap:12px; flex-wrap:wrap; align-items:end; }
    label { font-size: 12px; color: #666; }
    select, button { padding: 8px 12px; border-radius: 10px; border: 1px solid #ddd; background: #fff; }
    button { cursor:pointer; }
    button:hover { background:#fafafa; }
    .muted { color:#666; font-size: 12px; }
    a { color: #0b57d0; }
  </style>
</head>
<body>

  <div class="topbar">
    <div>
      <h2 style="margin:0;">Charts</h2>
      <div class="muted" style="margin-top:6px;">
        <a href="/">Home</a>
      </div>
    </div>
    <div class="muted">Source: Google Sheets → Events</div>
  </div>

  <div class="card" style="margin-top:14px;">
    <div class="row">
      <div>
        <label>Device</label><br/>
        <select id="device">
          {% for d in devices %}
            <option value="{{ d.device_id }}" {% if d.device_id == default_device %}selected{% endif %}>
              {{ (d.device_name or 'Device') }} ({{ d.device_id }})
            </option>
          {% endfor %}
        </select>
      </div>

      <div>
        <label>Event type</label><br/>
        <select id="msg_type">
          <option value="offline" selected>offline</option>
          <option value="online">online</option>
        </select>
      </div>

      <div>
        <button onclick="refreshAll()">Refresh</button>
      </div>

      <div class="muted" id="meta" style="padding-bottom:6px;"></div>
    </div>
  </div>

  <div class="card" style="margin-top:14px;">
    <canvas id="chart" height="120"></canvas>
  </div>

  <div class="card" style="margin-top:14px;">
    <div class="muted" id="ratio_meta" style="margin-bottom:8px;"></div>
    <canvas id="ratioChart" height="120"></canvas>
  </div>

  <div class="card" style="margin-top:14px;">
    <div class="muted" id="daily_meta" style="margin-bottom:8px;"></div>
    <canvas id="dailyChart" height="140"></canvas>
  </div>

<script>
let chartObj = null;
let ratioObj = null;
let dailyObj = null;

async function reloadChart(){
  const device = document.getElementById('device').value;
  const msgType = document.getElementById('msg_type').value;

  const metaEl = document.getElementById('meta');
  metaEl.textContent = 'Loading...';

  const res = await fetch(`/api/charts/minute-hist?device_id=${encodeURIComponent(device)}&msg_type=${encodeURIComponent(msgType)}`);
  const data = await res.json();

  if (!data.ok){
    metaEl.textContent = 'Error: ' + (data.error || 'unknown');
    return;
  }

  const minOut = data.min_outage_minutes ?? 10;
  metaEl.textContent = `Total (filtered): ${data.total} | ignore outages < ${minOut} min | source: ${data.source || 'unknown'}`;

  const labels = data.minutes.map(m => String(m).padStart(2,'0'));
  const counts = data.counts;
  const smoothed = data.smoothed;

  const ctx = document.getElementById('chart').getContext('2d');
  if (chartObj) chartObj.destroy();

  chartObj = new Chart(ctx, {
    data: {
      labels: labels,
      datasets: [
        { type: 'bar', label: `Count (${data.msg_type})`, data: counts },
        { type: 'line', label: 'Smoothed', data: smoothed, tension: 0.35, pointRadius: 0 }
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: { display: true, text: `Distribution of Minutes (${data.msg_type}) — ${data.device_id}` },
        tooltip: { mode: 'index', intersect: false }
      },
      interaction: { mode: 'index', intersect: false },
      scales: {
        y: { beginAtZero: true, title: { display: true, text: 'Frequency' } },
        x: { title: { display: true, text: 'Minute of the hour' } }
      }
    }
  });
}

async function reloadRatio(){
  const metaEl = document.getElementById('ratio_meta');
  metaEl.textContent = 'Loading ratio...';

  const res = await fetch('/api/charts/power-ratio');
  const data = await res.json();

  if (!data.ok){
    metaEl.textContent = 'Error: ' + (data.error || 'unknown');
    return;
  }

  metaEl.textContent = `${data.device_name} (${data.device_id}) | online: ${data.online_hours}h, offline: ${data.offline_hours}h | from ${data.range_from || '-'} to ${data.range_to || '-'}`;

  const ctx = document.getElementById('ratioChart').getContext('2d');
  if (ratioObj) ratioObj.destroy();

  ratioObj = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Light ON (hours)', 'Light OFF (hours)'],
      datasets: [{
        data: [data.online_hours, data.offline_hours]
      }]
    },
    options: {
      responsive: true,
      plugins: {
        title: { display: true, text: 'Power availability ratio (Parking) — hours' },
        tooltip: { mode: 'nearest' }
      }
    }
  });
}


async function reloadDaily(){
  const metaEl = document.getElementById('daily_meta');
  metaEl.textContent = 'Loading daily...';

  const res = await fetch('/api/charts/power-daily');
  const data = await res.json();

  if (!data.ok){
    metaEl.textContent = 'Error: ' + (data.error || 'unknown');
    return;
  }

  const n = (data.dates || []).length;
  metaEl.textContent = `${data.device_name} (${data.device_id}) | days: ${n} | from ${data.range_from || '-'} to ${data.range_to || '-'}`;

  const ctx = document.getElementById('dailyChart').getContext('2d');
  if (dailyObj) dailyObj.destroy();

  dailyObj = new Chart(ctx, {
    data: {
      labels: data.dates,
      datasets: [
        { type: 'bar', label: 'Light ON (hours)', data: data.online_hours, stack: 'hours', backgroundColor: 'rgba(0, 200, 83, 0.55)' },
        { type: 'bar', label: 'Light OFF (hours)', data: data.offline_hours, stack: 'hours', backgroundColor: 'rgba(244, 67, 54, 0.55)' },
        { type: 'line', label: 'OFF (%)', data: data.offline_pct, yAxisID: 'y1', tension: 0.35, pointRadius: 0, borderColor: 'rgba(244, 67, 54, 0.9)' }
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: { display: true, text: 'Power availability by date (Parking)' },
        tooltip: { mode: 'index', intersect: false }
      },
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: {
          stacked: true,
          ticks: { autoSkip: true, maxTicksLimit: 14, maxRotation: 0 }
        },
        y: {
          stacked: true,
          beginAtZero: true,
          title: { display: true, text: 'Hours' }
        },
        y1: {
          beginAtZero: true,
          min: 0,
          max: 100,
          position: 'right',
          grid: { drawOnChartArea: false },
          title: { display: true, text: 'OFF (%)' }
        }
      }
    }
  });
}

function refreshAll(){
  reloadChart();
  reloadRatio();
  reloadDaily();
}

refreshAll();
</script>

</body>
</html>
        """,
        devices=devices,
        default_device=default_device,
    )


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
        abort(400, description="Set IMOU_DEVICE_IDS (comma-separated) to use /admin/sync")

    details = imou_list_device_details_by_ids(IMOU_DEVICE_IDS)
    for d in details:
        device_id = str(d.get("deviceId", "")).strip()
        if not device_id:
            continue

        device_name = d.get("deviceName") or ""
        device_status = d.get("deviceStatus") or "unknown"

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
            last_seen_utc=now_kyiv_iso(),
        )

        try:
            online = imou_device_online(device_id)
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
