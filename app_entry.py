"""Application entrypoint with Telegram command handling.

Wraps the existing Flask app from ``main.py`` without changing the current
IMOU/OpenClaw/DTEK logic. Adds Telegram Bot API webhook commands:

- /graph - fetch and send the second DTEK outage schedule graph
- /id    - show Telegram user/chat IDs for access configuration
"""

import hashlib
import json
import os
import threading
import time

import requests

import main

app = main.app


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


def _authorized_for_commands(user_id: int) -> bool:
    explicit = _configured_command_user_ids()
    if explicit:
        return user_id in explicit
    return _is_channel_admin(user_id)


def _webhook_secret() -> str:
    explicit = (os.getenv("TELEGRAM_WEBHOOK_SECRET", "") or "").strip()
    if explicit:
        return explicit
    token = _telegram_token()
    if not token:
        return ""
    return hashlib.sha256(("imou-telegram-webhook:" + token).encode("utf-8")).hexdigest()


def _webhook_base_url() -> str:
    base = (os.getenv("TELEGRAM_WEBHOOK_BASE_URL", "") or "").strip().rstrip("/")
    if base:
        return base
    base = (main.PUBLIC_BASE_URL or "").strip().rstrip("/")
    if base:
        return base
    railway_domain = (os.getenv("RAILWAY_PUBLIC_DOMAIN", "") or "").strip().strip("/")
    if railway_domain:
        return f"https://{railway_domain}"
    return ""


def _graph_worker(chat_id, user_id: int):
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
        app.logger.warning("Telegram /graph DTEK fetch failed for user %s: %s", user_id, result)
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
    message = update.get("message") or update.get("channel_post") or {}
    text = str(message.get("text") or "").strip()
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    chat_id = chat.get("id")
    user_id = sender.get("id")

    if not chat_id or not text.startswith("/"):
        return {"ok": True}

    first = text.split(None, 1)[0]
    command = first.split("@", 1)[0].casefold()

    if command == "/id":
        _send_message(chat_id, f"Ваш Telegram user ID: {user_id}\nChat ID: {chat_id}")
        return {"ok": True}

    if command in {"/start", "/help"}:
        _send_message(
            chat_id,
            "Доступні команди:\n/graph — отримати другий графік відключень ДТЕК\n/id — показати Telegram user ID",
        )
        return {"ok": True}

    if command != "/graph":
        return {"ok": True}

    if not user_id or not _authorized_for_commands(int(user_id)):
        _send_message(
            chat_id,
            "⛔ Команда недоступна для цього користувача.\n"
            f"Ваш Telegram user ID: {user_id}\n"
            "Додайте його в Railway Variables: TELEGRAM_COMMAND_USER_IDS.",
        )
        return {"ok": True}

    if not main.DTEK_FORECAST_ENABLED:
        _send_message(chat_id, "⚠️ Інтеграція ДТЕК вимкнена (DTEK_FORECAST_ENABLED=0).")
        return {"ok": True}

    _send_message(chat_id, "⏳ Отримую актуальний графік ДТЕК…")
    worker = threading.Thread(
        target=_graph_worker,
        args=(chat_id, int(user_id)),
        name="telegram-dtek-graph",
        daemon=True,
    )
    worker.start()
    return {"ok": True}


def _register_telegram_webhook():
    if not main.telegram_enabled():
        app.logger.info("Telegram webhook not registered: Telegram notifications are disabled")
        return

    base = _webhook_base_url()
    if not base:
        app.logger.warning(
            "Telegram webhook not registered: set PUBLIC_BASE_URL, TELEGRAM_WEBHOOK_BASE_URL, or RAILWAY_PUBLIC_DOMAIN"
        )
        return

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
        return

    commands = [
        {"command": "graph", "description": "Отримати другий графік відключень ДТЕК"},
        {"command": "id", "description": "Показати Telegram user ID"},
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
