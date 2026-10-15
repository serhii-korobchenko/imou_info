"""Application entrypoint with Telegram command handling.

Wraps the existing Flask app from ``main.py`` without changing the current
IMOU/OpenClaw/DTEK logic. Adds Telegram Bot API webhook commands that work both
in a private chat with the bot and directly in the configured Telegram channel.

Also adds an in-process deduplication guard for repeated IMOU online/offline
callbacks. IMOU may deliver the same status transition more than once almost
simultaneously; without a guard, concurrent Gunicorn threads can both observe
the old status and both send the same Telegram notification.

Commands:
- /graph - fetch and send the second DTEK outage schedule graph
- /id    - show Telegram user/chat IDs for diagnostics
"""

import hashlib
import json
import os
import threading
import time

import requests

import main

app = main.app


# ---------------------------------------------------------------------------
# IMOU status notification deduplication
# ---------------------------------------------------------------------------
# The main callback already checks prev_status != status, but two callbacks can
# arrive concurrently and both read the same previous DB value before either
# request writes the new status. That race produces two identical Telegram
# messages (for example two "🟢 ДАЛИ СВІТЛО" messages at the same moment).
#
# Keep the original implementation intact and guard only the notification call.
# The state stores the most recently *accepted* status per device. A real
# online -> offline -> online flap is still allowed, because the intermediate
# status changes the stored value. Only repeated identical callbacks are
# suppressed within the short window.
_IMOU_NOTIFY_DEDUP_LOCK = threading.Lock()
_IMOU_NOTIFY_LAST = {}
_IMOU_NOTIFY_DEDUP_SEC = max(1, int(os.getenv("TELEGRAM_STATUS_DEDUP_SEC", "30")))
_original_maybe_notify_telegram_device_status = main.maybe_notify_telegram_device_status


def _dedup_maybe_notify_telegram_device_status(device_id: str, status: str, interval_note: str = ""):
    st = main._normalize_status(status)
    now_ts = time.time()

    if device_id and st in ("online", "offline"):
        with _IMOU_NOTIFY_DEDUP_LOCK:
            previous = _IMOU_NOTIFY_LAST.get(device_id)
            if previous:
                prev_status, prev_ts = previous
                if prev_status == st and (now_ts - float(prev_ts)) < _IMOU_NOTIFY_DEDUP_SEC:
                    app.logger.info(
                        "Suppressing duplicate IMOU Telegram status notification: device_id=%s status=%s age=%.3fs",
                        device_id,
                        st,
                        now_ts - float(prev_ts),
                    )
                    return

            # Record before the network send so a second concurrent callback is
            # blocked immediately rather than waiting for Telegram HTTP I/O.
            _IMOU_NOTIFY_LAST[device_id] = (st, now_ts)

    return _original_maybe_notify_telegram_device_status(device_id, status, interval_note)


# imou_callback() resolves this name from the main module at runtime, so replacing
# it here protects the existing callback without rewriting main.py.
main.maybe_notify_telegram_device_status = _dedup_maybe_notify_telegram_device_status


def _telegram_token() -> str:
    return (main.TELEGRAM_BOT_TOKEN or "").strip()


def _telegram_api(method: str, *, data=None, files=None, timeout=None) -> dict:
    token = _telegram_token()
    if not token:
        return {"ok": False, "description": "TELEGRAM_BOT_TOKEN is not configured"}
    try:
        url = f"https://api.telegram.org/bot{token}/{method}"
        r = requests.post(
            url,
            data=data,
            files=files,
            timeout=timeout or max(10, int(main.TELEGRAM_TIMEOUT_SEC or 10)),
        )
        try:
            payload = r.json()
        except Exception:
            payload = {"ok": False, "description": r.text[:500]}
        if r.status_code >= 400 and isinstance(payload, dict):
            payload.setdefault("ok", False)
            payload.setdefault("http_status", r.status_code)
        return payload if isinstance(payload, dict) else {"ok": False, "raw": payload}
    except Exception as exc:
        return {"ok": False, "description": str(exc)}


def _send_message(chat_id, text: str) -> dict:
    return _telegram_api(
        "sendMessage",
        data={
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": "true",
        },
    )


def _send_photo(chat_id, photo_path: str, caption: str = "") -> dict:
    if not photo_path or not os.path.exists(photo_path):
        return {"ok": False, "description": "graph image file was not found"}
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
    try:
        with open(photo_path, "rb") as fh:
            return _telegram_api(
                "sendPhoto",
                data=data,
                files={"photo": fh},
                timeout=max(30, int(main.TELEGRAM_TIMEOUT_SEC or 10)),
            )
    except Exception as exc:
        return {"ok": False, "description": str(exc)}


def _configured_command_user_ids() -> set[int]:
    raw = (os.getenv("TELEGRAM_COMMAND_USER_IDS", "") or "").strip()
    out = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            pass
    return out


def _configured_destination_matches(chat: dict) -> bool:
    """True when this Telegram chat is the configured IMOU destination channel/chat."""
    target = (main.TELEGRAM_CHAT_ID or "").strip()
    if not target or not isinstance(chat, dict):
        return False

    chat_id = chat.get("id")
    username = (chat.get("username") or "").strip()

    try:
        if chat_id is not None and str(int(chat_id)) == str(int(target)):
            return True
    except Exception:
        pass

    if target.startswith("@") and username:
        return username.casefold() == target[1:].casefold()
    if username:
        return username.casefold() == target.lstrip("@").casefold()

    return False


def _is_channel_admin(user_id: int) -> bool:
    target = (main.TELEGRAM_CHAT_ID or "").strip()
    if not target or not user_id:
        return False
    res = _telegram_api(
        "getChatMember",
        data={"chat_id": target, "user_id": str(user_id)},
    )
    if not res.get("ok"):
        return False
    member = res.get("result") or {}
    return member.get("status") in {"creator", "administrator"}


def _authorized_for_commands(*, chat: dict, user_id, is_channel_post: bool) -> bool:
    if is_channel_post and _configured_destination_matches(chat):
        return True

    try:
        uid = int(user_id)
    except Exception:
        return False

    explicit = _configured_command_user_ids()
    if explicit:
        return uid in explicit
    return _is_channel_admin(uid)


def _webhook_secret() -> str:
    explicit = (os.getenv("TELEGRAM_WEBHOOK_SECRET", "") or "").strip()
    if explicit:
        return explicit
    token = _telegram_token()
    if not token:
        return ""
    return hashlib.sha256(("imou-telegram-webhook:" + token).encode("utf-8")).hexdigest()


def _normalize_https_base_url(value: str) -> str:
    value = (value or "").strip().rstrip("/")
    if not value:
        return ""
    if value.startswith("https://"):
        return value
    if value.startswith("http://"):
        return "https://" + value[len("http://"):]
    return "https://" + value


def _webhook_base_url() -> str:
    base = _normalize_https_base_url(os.getenv("TELEGRAM_WEBHOOK_BASE_URL", ""))
    if base:
        return base
    base = _normalize_https_base_url(main.PUBLIC_BASE_URL)
    if base:
        return base
    railway_domain = (os.getenv("RAILWAY_PUBLIC_DOMAIN", "") or "").strip().strip("/")
    if railway_domain:
        return _normalize_https_base_url(railway_domain)
    return ""


def _graph_worker(chat_id, actor_label: str):
    try:
        result = main.dtek_get_schedule_graph_second(force=True)
        if isinstance(result, dict) and result.get("ok") and result.get("path"):
            sent = _send_photo(
                chat_id,
                result["path"],
                caption="📈 ДТЕК — графік відключень (другий графік)",
            )
            if not sent.get("ok"):
                _send_message(chat_id, "⚠️ Графік отримано від ДТЕК, але не вдалося надіслати JPG у Telegram.")
                app.logger.warning("Telegram /graph sendPhoto failed: %s", sent)
            return

        note = ""
        if isinstance(result, dict):
            note = str(result.get("note") or result.get("error") or "").strip()
        _send_message(
            chat_id,
            f"⚠️ Не вдалося отримати графік ДТЕК: {note}" if note else "⚠️ Не вдалося отримати графік ДТЕК. Спробуйте пізніше.",
        )
        app.logger.warning("Telegram /graph DTEK fetch failed for %s: %s", actor_label, result)
    except Exception as exc:
        app.logger.exception("Telegram /graph worker failed")
        _send_message(chat_id, f"⚠️ Помилка під час отримання графіка ДТЕК: {exc}")


@app.post("/telegram/webhook")
def telegram_command_webhook():
    expected = _webhook_secret()
    if expected:
        supplied = (main.request.headers.get("X-Telegram-Bot-Api-Secret-Token") or "").strip()
        if supplied != expected:
            return {"ok": False}, 403

    update = main.request.get_json(silent=True) or {}
    is_channel_post = bool(update.get("channel_post"))
    message = update.get("message") or update.get("channel_post") or {}
    text = str(message.get("text") or "").strip()
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    sender_chat = message.get("sender_chat") or {}
    chat_id = chat.get("id")
    user_id = sender.get("id")

    if not chat_id or not text.startswith("/"):
        return {"ok": True}

    first = text.split(None, 1)[0]
    command = first.split("@", 1)[0].casefold()

    app.logger.info(
        "Telegram command received: command=%s chat_id=%s channel_post=%s user_id=%s sender_chat_id=%s",
        command,
        chat_id,
        is_channel_post,
        user_id,
        sender_chat.get("id"),
    )

    if command == "/id":
        username = chat.get("username") or ""
        lines = [f"Chat ID: {chat_id}"]
        if username:
            lines.append(f"Chat: @{username}")
        if user_id:
            lines.insert(0, f"Ваш Telegram user ID: {user_id}")
        elif is_channel_post:
            lines.insert(0, "Команда надіслана від імені каналу; user ID у channel_post відсутній.")
        _send_message(chat_id, "\n".join(lines))
        return {"ok": True}

    if command in {"/start", "/help"}:
        _send_message(
            chat_id,
            "Доступні команди:\n/graph — отримати другий графік відключень ДТЕК\n/id — показати Telegram user/chat ID",
        )
        return {"ok": True}

    if command != "/graph":
        return {"ok": True}

    if not _authorized_for_commands(chat=chat, user_id=user_id, is_channel_post=is_channel_post):
        _send_message(
            chat_id,
            "⛔ Команда /graph недоступна в цьому чаті.\n"
            f"Chat ID: {chat_id}\n"
            f"User ID: {user_id}",
        )
        return {"ok": True}

    if not main.DTEK_FORECAST_ENABLED:
        _send_message(chat_id, "⚠️ Інтеграція ДТЕК вимкнена (DTEK_FORECAST_ENABLED=0).")
        return {"ok": True}

    _send_message(chat_id, "⏳ Отримую актуальний графік ДТЕК…")
    actor = f"channel {chat_id}" if is_channel_post else f"user {user_id}"
    worker = threading.Thread(
        target=_graph_worker,
        args=(chat_id, actor),
        name="telegram-dtek-graph",
        daemon=True,
    )
    worker.start()
    return {"ok": True}


@app.get("/admin/telegram-webhook-info")
def admin_telegram_webhook_info():
    main.require_admin()
    return main.jsonify(_telegram_api("getWebhookInfo", timeout=20))


@app.post("/admin/telegram-webhook-register")
def admin_telegram_webhook_register():
    main.require_admin()
    return main.jsonify(_register_telegram_webhook())


def _register_telegram_webhook() -> dict:
    if not main.telegram_enabled():
        res = {"ok": False, "description": "Telegram notifications are disabled"}
        app.logger.info("Telegram webhook not registered: %s", res["description"])
        return res

    base = _webhook_base_url()
    if not base:
        res = {
            "ok": False,
            "description": "set PUBLIC_BASE_URL, TELEGRAM_WEBHOOK_BASE_URL, or RAILWAY_PUBLIC_DOMAIN",
        }
        app.logger.warning("Telegram webhook not registered: %s", res["description"])
        return res

    secret = _webhook_secret()
    data = {
        "url": f"{base}/telegram/webhook",
        "allowed_updates": json.dumps(["message", "channel_post"]),
        "drop_pending_updates": "false",
    }
    if secret:
        data["secret_token"] = secret

    res = _telegram_api("setWebhook", data=data, timeout=20)
    if not res.get("ok"):
        app.logger.warning("Telegram setWebhook failed: %s", res)
        return res

    commands = [
        {"command": "graph", "description": "Отримати другий графік відключень ДТЕК"},
        {"command": "id", "description": "Показати Telegram user/chat ID"},
    ]
    cmd_res = _telegram_api(
        "setMyCommands",
        data={"commands": json.dumps(commands, ensure_ascii=False)},
        timeout=20,
    )
    if not cmd_res.get("ok"):
        app.logger.warning("Telegram setMyCommands failed: %s", cmd_res)
    else:
        app.logger.info("Telegram command webhook ready at %s/telegram/webhook", base)

    return {"ok": True, "webhook": res, "commands": cmd_res, "url": f"{base}/telegram/webhook"}


def _delayed_register_webhook():
    time.sleep(1.0)
    try:
        _register_telegram_webhook()
    except Exception:
        app.logger.exception("Telegram webhook registration failed")


threading.Thread(
    target=_delayed_register_webhook,
    name="telegram-webhook-register",
    daemon=True,
).start()
